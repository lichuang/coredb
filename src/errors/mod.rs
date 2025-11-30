use crate::protocol::ParseError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  #[error("Raft fatal error")]
  OpenRaftError(#[from] crate::raft::OpenRaftError),

  #[error("Config error")]
  Config(#[from] crate::config::ConfigError),

  #[error("Redis protocol error: {0}")]
  RedisProtocol(String),

  #[error("Connection error: {0}")]
  Connection(String),

  #[error("Parse client request error: {0}")]
  ParseRequest(#[from] ParseError),

  #[error("Network error: {0}")]
  Network(NetworkError),

  #[error("Tokio Runtime error: {0}")]
  TokioRuntime(#[from] TokioRuntimeError),
}

impl Error {
  pub fn dns_parse_error(e: String) -> Self {
    Self::Network(NetworkError::DnsParse(e))
  }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

mod network_errors;
pub use network_errors::NetworkError;

mod runtime_errors;
pub use runtime_errors::TokioRuntimeError;
