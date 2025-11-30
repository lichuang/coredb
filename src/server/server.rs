use std::net::Ipv4Addr;
use std::sync::Arc;

use anyerror::AnyError;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::error;
use tracing::info;

use super::shutdown::Shutdown;
use crate::config::Config;
use crate::config::GrpcConfig;
use crate::errors::Error;
use crate::errors::Result;
use crate::raft::Raft;
use crate::raft::RaftServiceImpl;
use crate::server::connection::Connection;
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
}

impl Server {
  pub(crate) async fn run(&mut self) -> Result<()> {
    info!("accepting connections");

    loop {
      let (socket, client_addr) = self.listener.accept().await?;
      info!("accept connection from {:?}", client_addr);

      let mut connection = Connection::new(socket, Shutdown::new(self.notify_shutdown.subscribe()));

      tokio::spawn(async move {
        if let Err(err) = connection.run().await {
          error!(cause = ?err, "connection error");
        }
      });
    }
    Ok(())
  }

  pub(crate) async fn start(&self) -> Result<()> {
    self.start_raft_service().await
  }

  async fn start_raft_service(&self) -> Result<()> {
    let config = &self.config;

    let host = &config.raft_host;
    let port = &config.raft_port;
    info!("Start raft service listening on: {}:{}", host, port);

    let raft_service_impl = RaftServiceImpl::new(self.raft.clone());
    let raft_server = RaftServiceServer::new(raft_service_impl)
      .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
      .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);

    let ipv4_addr = host.parse::<Ipv4Addr>();
    let ip_port = match ipv4_addr {
      Ok(addr) => format!("{}:{}", addr, port),
      Err(_) => {
        let resolver = DNSResolver::instance()
          .map_err(|e| Error::Network(format!("get dns resolver instance error: {}", e)))?;
        let ip_addrs = resolver
          .resolve(host.clone())
          .await
          .map_err(|e| Error::Network(format!("resolve addr {} error: {}", host, e)))?;
        format!("{}:{}", ip_addrs[0], port)
      }
    };

    info!("about to start raft grpc on: {}", ip_port);

    let socket_addr = ip_port
      .parse::<std::net::SocketAddr>()
      .map_err(|e| Error::Network(format!("Parse addr {} error: {}", ip_port, e)))?;
    let node_id = config.node_id;

    let srv = tonic::transport::Server::builder().add_service(raft_server);
    let mut running_rx = self.running_rx.clone();

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

    let mut jh = self.join_handles.lock().await;
    jh.push(h);

    Ok(())
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
