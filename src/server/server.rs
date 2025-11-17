use std::net::Ipv4Addr;
use std::sync::Arc;

use anyerror::AnyError;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
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
use crate::raft::new_raft;
use crate::raft::protobuf::raft_service_server::RaftServiceServer;
use crate::server::connection::Connection;
use crate::util::DNSResolver;

const DEFAULT_PORT: u16 = 6379;

pub struct Server {
  listener: TcpListener,

  notify_shutdown: broadcast::Sender<()>,

  shutdown_complete_tx: mpsc::Sender<()>,

  pub running_tx: watch::Sender<()>,
  pub running_rx: watch::Receiver<()>,

  pub join_handles: Mutex<Vec<JoinHandle<Result<(), AnyError>>>>,

  raft: Arc<Raft>,
}

impl Server {
  async fn run(&mut self) -> Result<()> {
    info!("accepting connections");

    loop {
      let (socket, client_addr) = self.listener.accept().await?;
      info!("accept connection from {:?}", client_addr);

      let mut connection = Connection::new(
        socket,
        Shutdown::new(self.notify_shutdown.subscribe()),
        self.shutdown_complete_tx.clone(),
      );

      tokio::spawn(async move {
        if let Err(err) = connection.run().await {
          error!(cause = ?err, "connection error");
        }
      });
    }
    Ok(())
  }
}

async fn start_raft_service(server: Arc<Server>, config: &Config) -> Result<()> {
  let host = &config.raft_host;
  let port = &config.raft_port;
  info!("Start raft service listening on: {}:{}", host, port);

  let raft_service_impl = RaftServiceImpl::new(server.raft.clone());
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

pub async fn run(config: Config, shutdown: impl Future) -> Result<()> {
  println!("listen: {}", DEFAULT_PORT);
  let listener = TcpListener::bind(&format!("127.0.0.1:{}", DEFAULT_PORT)).await?;

  let (notify_shutdown, _) = broadcast::channel(1);
  let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

  let raft = new_raft(&config).await?;

  let (tx, rx) = watch::channel::<()>(());

  let mut server = Server {
    listener,
    notify_shutdown,
    shutdown_complete_tx,
    running_tx: tx,
    running_rx: rx,
    join_handles: Mutex::new(Vec::new()),
    raft: Arc::new(raft),
  };

  tokio::select! {
      res = server.run() => {
          if let Err(err) = res {
              error!(cause = %err, "failed to accept");
          }
      }
      _ = shutdown => {
          // The shutdown signal has been received.
          info!("coredb server shutting down");
      }
  }

  let Server {
    shutdown_complete_tx,
    notify_shutdown,
    running_tx,
    ..
  } = server;

  drop(notify_shutdown);
  drop(shutdown_complete_tx);

  let _ = shutdown_complete_rx.recv().await;
  running_tx.send(()).unwrap();

  Ok(())
}
