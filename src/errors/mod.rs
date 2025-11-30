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

  #[error("Dns Parse error: {0}")]
  DnsParse(String),

  #[error("Network error: {0}")]
  Network(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
