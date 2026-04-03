use crate::encoding::StringValue;
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use async_trait::async_trait;
use rockraft::raft::types::UpsertKV;

/// Parameters for MSET command
#[derive(Debug, Clone, PartialEq)]
pub struct MsetParams {
  pub pairs: Vec<(String, Vec<u8>)>,
}

impl MsetParams {
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() < 3 {
      return Err(ProtocolError::WrongArgCount("MSET"));
    }

    let args_count = items.len() - 1;
    if !args_count.is_multiple_of(2) {
      return Err(ProtocolError::WrongArgCount("MSET"));
    }

    let mut pairs = Vec::with_capacity(args_count / 2);
    let mut i = 1;
    while i < items.len() {
      let key = match &items[i] {
        Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
        Value::SimpleString(s) => s.clone(),
        _ => return Err(ProtocolError::InvalidArgument("key")),
      };

      let value = match &items[i + 1] {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Err(ProtocolError::InvalidArgument("value")),
      };

      pairs.push((key, value));
      i += 2;
    }

    Ok(MsetParams { pairs })
  }
}

/// MSET command executor
pub struct MsetCommand;

#[async_trait]
impl Command for MsetCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = MsetParams::parse(items)?;

    let mut entries: Vec<UpsertKV> = Vec::with_capacity(params.pairs.len());

    for (key, value) in params.pairs {
      let string_value = StringValue::new(value);
      let serialized = string_value.serialize();
      entries.push(UpsertKV::insert(&key, &serialized));
    }

    server.batch_write(entries).await?;
    Ok(Value::ok())
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
    assert!(MsetParams::parse(&items).is_err());
  }

  #[test]
  fn test_mset_params_parse_missing_value() {
    let items = vec![
      Value::BulkString(Some(b"MSET".to_vec())),
      Value::BulkString(Some(b"key1".to_vec())),
    ];
    assert!(MsetParams::parse(&items).is_err());
  }

  #[test]
  fn test_mset_params_parse_odd_arguments() {
    let items = vec![
      Value::BulkString(Some(b"MSET".to_vec())),
      Value::BulkString(Some(b"key1".to_vec())),
      Value::BulkString(Some(b"value1".to_vec())),
      Value::BulkString(Some(b"key2".to_vec())),
    ];
    assert!(MsetParams::parse(&items).is_err());
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
      Value::Integer(123),
      Value::BulkString(Some(b"value1".to_vec())),
    ];
    assert!(MsetParams::parse(&items).is_err());
  }

  #[test]
  fn test_mset_params_parse_invalid_value_type() {
    let items = vec![
      Value::BulkString(Some(b"MSET".to_vec())),
      Value::BulkString(Some(b"key1".to_vec())),
      Value::Integer(123),
    ];
    assert!(MsetParams::parse(&items).is_err());
  }
}
