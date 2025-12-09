use bytes::Bytes;

use super::server::Server;
use crate::errors::Result;
use crate::types::protobuf::KvMeta;
use crate::types::protobuf::SetRequest;

#[async_trait::async_trait]
pub trait KVApi: Send + Sync {
  async fn put(&self, key: &str, value: &Bytes, meta: Option<KvMeta>) -> Result<()>;

  async fn get(&self, key: &str) -> Result<Bytes>;
}

#[async_trait::async_trait]
impl KVApi for Server {
  async fn put(&self, key: &str, value: &Bytes, meta: Option<KvMeta>) -> Result<()> {
    let req = SetRequest {
      key: key.to_string(),
      value: value.to_vec(),
      meta: meta,
    };

    self.write(req).await?;
    Ok(())
  }

  async fn get(&self, key: &str) -> Result<Bytes> {
    Ok(Bytes::default())
  }
}
