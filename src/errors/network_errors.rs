use std::net::AddrParseError;

// represent network related errors
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum NetworkError {
  #[error(transparent)]
  AddrParse(#[from] AddrParseError),

  #[error("{0}")]
  DnsParse(String),
}

impl From<AddrParseError> for crate::errors::Error {
  fn from(e: AddrParseError) -> Self {
    crate::errors::Error::Network(NetworkError::AddrParse(e))
  }
}
