use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyerror::AnyError;
use anyerror::func_name;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::raft_leader::RaftLeader;
use super::shutdown::Shutdown;
use super::store::RaftStore;
use crate::config::Config;
use crate::config::GrpcConfig;
use crate::errors::RaftAPIError;
use crate::errors::Result;
use crate::raft::ForwardToLeader;
use crate::raft::NodeId;
use crate::raft::Raft;
use crate::raft::RaftServiceImpl;
use crate::server::connection::Connection;
use crate::server::raft_forwarder::RaftForwarder;
use crate::types::protobuf::ForwardRequest;
use crate::types::protobuf::ForwardResponse;
use crate::types::protobuf::RaftRequest;
use crate::types::protobuf::Response;
use crate::types::protobuf::SetRequest;
use crate::types::protobuf::raft_request::Data;
use crate::types::protobuf::raft_service_server::RaftServiceServer;
use crate::util::DNSResolver;

pub struct Server {
  pub listener: TcpListener,

  pub config: Config,

  pub notify_shutdown: broadcast::Sender<()>,

  pub running_tx: watch::Sender<()>,
  pub running_rx: watch::Receiver<()>,

  pub join_handles: Mutex<Vec<JoinHandle<Result<(), AnyError>>>>,

  pub raft: Arc<Raft>,

  pub raft_store: RaftStore,
}

impl Server {
  pub(crate) async fn run(server: Arc<Server>) -> Result<()> {
    info!("accepting connections");

    loop {
      let (socket, client_addr) = server.listener.accept().await?;
      info!("accept connection from {:?}", client_addr);

      let mut connection =
        Connection::new(socket, Shutdown::new(server.notify_shutdown.subscribe()));

      tokio::spawn(async move {
        if let Err(err) = connection.run().await {
          error!(cause = ?err, "connection error");
        }
      });
    }
    Ok(())
  }

  pub(crate) async fn start(server: Arc<Server>) -> Result<()> {
    Self::start_raft_service(server).await
  }

  async fn start_raft_service(server: Arc<Server>) -> Result<()> {
    let config = &server.config;

    let host = &config.raft_host;
    let port = &config.raft_port;
    info!("Start raft service listening on: {}:{}", host, port);

    let raft_service_impl = RaftServiceImpl::new(server.clone());
    let raft_server = RaftServiceServer::new(raft_service_impl)
      .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
      .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);

    let ipv4_addr = host.parse::<Ipv4Addr>();
    let ip_port = match ipv4_addr {
      Ok(addr) => format!("{}:{}", addr, port),
      Err(_) => {
        let resolver = DNSResolver::instance()?;
        let ip_addrs = resolver.resolve(host.clone()).await?;
        format!("{}:{}", ip_addrs[0], port)
      }
    };

    info!("about to start raft grpc on: {}", ip_port);

    let socket_addr = ip_port.parse::<std::net::SocketAddr>()?;
    let node_id = config.node_id;

    let srv = tonic::transport::Server::builder().add_service(raft_server);
    let mut running_rx = server.running_rx.clone();

    let h = tokio::spawn(async move {
      srv
        .serve_with_shutdown(socket_addr, async move {
          let _ = running_rx.changed().await;
          info!(
            "running_rx for Raft server received, shutting down: id={} {} ",
            node_id, ip_port
          );
        })
        .await
        .map_err(|e| AnyError::new(&e).add_context(|| "when serving meta-service raft service"))?;

      Ok::<(), AnyError>(())
    });

    let mut jh = server.join_handles.lock().await;
    jh.push(h);

    Ok(())
  }

  async fn get_leader(&self) -> Result<Option<NodeId>> {
    let mut rx = self.raft.metrics();

    let mut expire_at: Option<Instant> = None;

    loop {
      if let Some(l) = rx.borrow().current_leader {
        return Ok(Some(l));
      }

      if expire_at.is_none() {
        let timeout = Duration::from_millis(2_000);
        expire_at = Some(Instant::now() + timeout);
      }
      if Some(Instant::now()) > expire_at {
        warn!("timeout waiting for a leader");
        return Ok(None);
      }

      // Wait for metrics to change and re-fetch the leader id.
      //
      // Note that when it returns, `changed()` will mark the most recent value as **seen**.
      rx.changed().await?;
    }
  }

  async fn assume_leader(&self) -> Result<RaftLeader<'_>, ForwardToLeader> {
    let leader_id = self.get_leader().await.map_err(|e| {
      error!("raft metrics rx closed: {}", e);
      ForwardToLeader {
        leader_id: None,
        leader_node: None,
      }
    })?;

    debug!("curr_leader_id: {:?}", leader_id);

    if leader_id == Some(self.config.node_id) {
      return Ok(RaftLeader::new(self));
    }

    Err(ForwardToLeader {
      leader_id,
      leader_node: None,
    })
  }

  /// Submit a write request to the known leader. Returns the response after applying the request.
  pub async fn write(&self, req: SetRequest) -> Result<Response> {
    debug!("{} req: {:?}", func_name!(), req);

    let raft_req = RaftRequest {
      data: Some(Data::Set(req)),
    };
    let forward_req = ForwardRequest {
      forward_to_leader: 1,
      request: Some(raft_req),
    };
    // TODO: enable returning endpoint
    let (_endpoint, res) = self.handle_forwardable_request(forward_req).await?;

    match res.response {
      Some(response) => Ok(Response {
        value: Some(response.error),
      }),
      None => Ok(Response { value: None }),
    }
  }

  pub async fn handle_forwardable_request(
    &self,
    req: ForwardRequest,
  ) -> Result<(Option<String>, ForwardResponse)> {
    let id = self.config.node_id;
    debug!(
      "id={} forward_quota={} handle_forwardable_request req={:?}",
      id, req.forward_to_leader, req
    );

    let mut req = req;
    let mut n_retry = 20;
    let mut slp = Duration::from_millis(1_000);

    loop {
      let assume_leader_res = self.assume_leader().await;
      debug!(
        "id={} assume_leader: is_err: {}",
        id,
        assume_leader_res.is_err()
      );

      // Handle the request locally or return a ForwardToLeader error
      let op_err = match assume_leader_res {
        Ok(leader) => {
          let res = leader.handle(req.clone()).await;
          match res {
            Ok(x) => return Ok((None, x)),
            Err(e) => e,
          }
        }
        Err(e) => RaftAPIError::ForwardToLeader(e),
      };

      // If it needs to forward, deal with it. Otherwise, return the unhandlable error.
      let to_leader = if let RaftAPIError::ForwardToLeader(err) = op_err {
        err
      } else {
        return Err(op_err.into());
      };

      let leader_id = to_leader.leader_id.ok_or_else(|| {
        RaftAPIError::CanNotForward(AnyError::error("need to forward but no known leader"))
      })?;
      req.forward_to_leader += 1;

      let forwarder = RaftForwarder::new(self);
      let resp = forwarder.forward(leader_id, &mut req).await;

      match resp {
        Err(err) => {
          if let RaftAPIError::NetworkError(err) = err {
            warn!(
              "{} retries left, sleep time: {:?}; forward_to {} failed: {}",
              n_retry, slp, leader_id, err
            );

            n_retry -= 1;
            if n_retry == 0 {
              error!("no more retry for forward_to {}", leader_id);
              let msg = format!("cannot forward to the leader due to network error: {}", err);
              return Err(RaftAPIError::CanNotForward(AnyError::error(msg)).into());
            } else {
              tokio::time::sleep(slp).await;
              slp = std::cmp::min(slp * 2, Duration::from_secs(1));
              continue;
            }
          } else {
            return Err(err.into());
          }
        }
        Ok(resp) => return Ok((None, resp)),
      }
    }
  }

  pub async fn get_node_endpoint(&self, node_id: &NodeId) -> Result<Option<String>> {
    Ok(self.raft_store.get_node(node_id)?.map(|n| n.endpoint))
  }

  pub async fn shutdown(&self) {
    drop(self.notify_shutdown.clone());

    self.running_tx.send(()).unwrap();

    for j in self.join_handles.lock().await.iter_mut() {
      let res = j.await;
      info!("task quit res: {:?}", res);

      // The returned error does not mean this function call failed.
      // Do not need to return this error. Keep shutting down other tasks.
      if let Err(ref e) = res {
        error!("task quit with error: {:?}", e);
      }
    }

    info!("shutdown: id={}", self.config.node_id);
  }
}
