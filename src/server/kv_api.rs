use bytes::Bytes;

use super::server::Server;
use crate::errors::Result;
use crate::types::cmd::Cmd;
use crate::types::cmd::InsertKV;
use crate::types::cmd::KVMeta;
use crate::types::log_entry::LogEntry;

#[async_trait::async_trait]
pub trait KVApi: Send + Sync {
  async fn put(&self, key: &str, value: Bytes, meta: Option<KVMeta>) -> Result<()>;

  async fn get(&self, key: &str) -> Result<Bytes>;
}

#[async_trait::async_trait]
impl KVApi for Server {
  async fn put(&self, key: &str, value: Bytes, meta: Option<KVMeta>) -> Result<()> {
    let cmd = Cmd::InsertKV(InsertKV::new(key, value.to_vec(), meta));
    let entry = LogEntry::new(cmd);
    Ok(())
  }

  async fn get(&self, key: &str) -> Result<Bytes> {
    Ok(Bytes::default())
  }
}
