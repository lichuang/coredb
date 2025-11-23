use bytes::Bytes;

use super::server::Server;
use crate::errors::Result;

#[async_trait::async_trait]
pub trait KVApi: Send + Sync {
  async fn put(&self, key: &str, value: Bytes) -> Result<()>;

  async fn get(&self, key: &str) -> Result<Bytes>;
}

#[async_trait::async_trait]
impl KVApi for Server {
  async fn put(&self, key: &str, value: Bytes) -> Result<()> {
    Ok(())
  }

  async fn get(&self, key: &str) -> Result<Bytes> {
    Ok(Bytes::default())
  }
}
