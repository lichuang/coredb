use crate::encoding::StringValue;
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use async_trait::async_trait;
use rockraft::raft::types::UpsertKV;

/// Parameters for MSET command
#[derive(Debug, Clone, PartialEq)]
pub struct MsetParams {
  /// Key-value pairs to set
  pub pairs: Vec<(String, Vec<u8>)>,
}

impl MsetParams {
  /// Parse MSET command parameters from RESP array items
  /// Format: MSET key value [key value ...]
  fn parse(items: &[Value]) -> Option<Self> {
    // Minimum: MSET + at least one key-value pair
    if items.len() < 3 {
      return None;
    }

    // Must have an even number of arguments after MSET
    // items[0] = MSET, items[1..] should be pairs
    let args_count = items.len() - 1;
    if !args_count.is_multiple_of(2) {
      return None;
    }

    let mut pairs = Vec::with_capacity(args_count / 2);
    let mut i = 1;
    while i < items.len() {
      // Parse key
      let key = match &items[i] {
        Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
        Value::SimpleString(s) => s.clone(),
        _ => return None,
      };

      // Parse value
      let value = match &items[i + 1] {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return None,
      };

      pairs.push((key, value));
      i += 2;
    }

    Some(MsetParams { pairs })
  }
}

/// MSET command executor
pub struct MsetCommand;

#[async_trait]
impl Command for MsetCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    let params = match MsetParams::parse(items) {
      Some(params) => params,
      None => return Value::error("ERR wrong number of arguments for 'mset' command"),
    };

    // Prepare batch write entries
    // MSET always overwrites existing values (no TTL preservation)
    let mut entries: Vec<UpsertKV> = Vec::with_capacity(params.pairs.len());

    for (key, value) in params.pairs {
      let string_value = StringValue::new(value);
      let serialized = string_value.serialize();
      entries.push(UpsertKV::insert(&key, &serialized));
    }

    // Atomic batch write - all or nothing
    if let Err(e) = server.batch_write(entries).await {
      return Value::error(format!("ERR batch write failed: {}", e));
    }

    Value::ok()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_mset_params_parse_single_pair() {
    let items = vec![
      Value::BulkString(Some(b"MSET".to_vec())),
      Value::BulkString(Some(b"key1".to_vec())),
      Value::BulkString(Some(b"value1".to_vec())),
    ];
    let params = MsetParams::parse(&items).unwrap();
    assert_eq!(params.pairs.len(), 1);
    assert_eq!(params.pairs[0].0, "key1");
    assert_eq!(params.pairs[0].1, b"value1");
  }

  #[test]
  fn test_mset_params_parse_multiple_pairs() {
    let items = vec![
      Value::BulkString(Some(b"MSET".to_vec())),
      Value::BulkString(Some(b"key1".to_vec())),
      Value::BulkString(Some(b"value1".to_vec())),
      Value::BulkString(Some(b"key2".to_vec())),
      Value::BulkString(Some(b"value2".to_vec())),
      Value::BulkString(Some(b"key3".to_vec())),
      Value::BulkString(Some(b"value3".to_vec())),
    ];
    let params = MsetParams::parse(&items).unwrap();
    assert_eq!(params.pairs.len(), 3);
    assert_eq!(params.pairs[0].0, "key1");
    assert_eq!(params.pairs[0].1, b"value1");
    assert_eq!(params.pairs[1].0, "key2");
    assert_eq!(params.pairs[1].1, b"value2");
    assert_eq!(params.pairs[2].0, "key3");
    assert_eq!(params.pairs[2].1, b"value3");
  }

  #[test]
  fn test_mset_params_parse_no_pairs() {
    let items = vec![Value::BulkString(Some(b"MSET".to_vec()))];
    assert!(MsetParams::parse(&items).is_none());
  }

  #[test]
  fn test_mset_params_parse_missing_value() {
    // Only key without value
    let items = vec![
      Value::BulkString(Some(b"MSET".to_vec())),
      Value::BulkString(Some(b"key1".to_vec())),
    ];
    assert!(MsetParams::parse(&items).is_none());
  }

  #[test]
  fn test_mset_params_parse_odd_arguments() {
    // key1 value1 key2 (missing value2)
    let items = vec![
      Value::BulkString(Some(b"MSET".to_vec())),
      Value::BulkString(Some(b"key1".to_vec())),
      Value::BulkString(Some(b"value1".to_vec())),
      Value::BulkString(Some(b"key2".to_vec())),
    ];
    assert!(MsetParams::parse(&items).is_none());
  }

  #[test]
  fn test_mset_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("MSET".to_string()),
      Value::SimpleString("key1".to_string()),
      Value::SimpleString("value1".to_string()),
    ];
    let params = MsetParams::parse(&items).unwrap();
    assert_eq!(params.pairs.len(), 1);
    assert_eq!(params.pairs[0].0, "key1");
    assert_eq!(params.pairs[0].1, b"value1");
  }

  #[test]
  fn test_mset_params_parse_mixed_types() {
    let items = vec![
      Value::BulkString(Some(b"MSET".to_vec())),
      Value::SimpleString("key1".to_string()),
      Value::BulkString(Some(b"value1".to_vec())),
      Value::BulkString(Some(b"key2".to_vec())),
      Value::SimpleString("value2".to_string()),
    ];
    let params = MsetParams::parse(&items).unwrap();
    assert_eq!(params.pairs.len(), 2);
    assert_eq!(params.pairs[0].0, "key1");
    assert_eq!(params.pairs[0].1, b"value1");
    assert_eq!(params.pairs[1].0, "key2");
    assert_eq!(params.pairs[1].1, b"value2");
  }

  #[test]
  fn test_mset_params_parse_empty_value() {
    let items = vec![
      Value::BulkString(Some(b"MSET".to_vec())),
      Value::BulkString(Some(b"key1".to_vec())),
      Value::BulkString(Some(b"".to_vec())),
    ];
    let params = MsetParams::parse(&items).unwrap();
    assert_eq!(params.pairs.len(), 1);
    assert_eq!(params.pairs[0].0, "key1");
    assert_eq!(params.pairs[0].1, b"");
  }

  #[test]
  fn test_mset_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"MSET".to_vec())),
      Value::Integer(123), // Invalid key type
      Value::BulkString(Some(b"value1".to_vec())),
    ];
    assert!(MsetParams::parse(&items).is_none());
  }

  #[test]
  fn test_mset_params_parse_invalid_value_type() {
    let items = vec![
      Value::BulkString(Some(b"MSET".to_vec())),
      Value::BulkString(Some(b"key1".to_vec())),
      Value::Integer(123), // Invalid value type
    ];
    assert!(MsetParams::parse(&items).is_none());
  }
}
