#[derive(thiserror::Error, Debug)]
pub enum TokioRuntimeError {
  #[error(transparent)]
  WatchRecvError(#[from] tokio::sync::watch::error::RecvError),
}

impl From<tokio::sync::watch::error::RecvError> for crate::errors::Error {
  fn from(e: tokio::sync::watch::error::RecvError) -> Self {
    crate::errors::Error::TokioRuntime(TokioRuntimeError::WatchRecvError(e))
  }
}
