use std::path::Path;
use std::sync::Arc;

pub use log_store::RocksLogStore;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::DB;
use rocksdb::Options;
pub use state_machine::RocksStateMachine;

use crate::types::raft::TypeConfig;

mod log_store;
mod meta;
mod state_machine;

/// Create a pair of `RocksLogStore` and `RocksStateMachine` that are backed by a same rocks db
/// instance.
pub(crate) async fn new_storage<P: AsRef<Path>>(
  db_path: P,
) -> Result<(RocksLogStore<TypeConfig>, RocksStateMachine), std::io::Error> {
  let mut db_opts = Options::default();
  db_opts.create_missing_column_families(true);
  db_opts.create_if_missing(true);

  let meta = ColumnFamilyDescriptor::new("meta", Options::default());
  let sm_meta = ColumnFamilyDescriptor::new("sm_meta", Options::default());
  let sm_data = ColumnFamilyDescriptor::new("sm_data", Options::default());
  let logs = ColumnFamilyDescriptor::new("logs", Options::default());

  let db_path = db_path.as_ref();
  let snapshot_dir = db_path.join("snapshots");

  let db = DB::open_cf_descriptors(&db_opts, db_path, vec![meta, sm_meta, sm_data, logs])
    .map_err(std::io::Error::other)?;

  let db = Arc::new(db);
  Ok((
    RocksLogStore::new(db.clone()),
    RocksStateMachine::new(db, snapshot_dir).await?,
  ))
}
