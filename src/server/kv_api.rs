use bytes::Bytes;

use crate::errors::Result;

#[async_trait::async_trait]
pub trait KVApi: Send + Sync {
  async fn put(&self, key: &str, value: Bytes) -> Result<()>;

  async fn get(&self, key: &str) -> Result<Bytes>;
}
