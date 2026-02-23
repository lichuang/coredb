use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::store::Store;
use async_trait::async_trait;

/// Parameters for SET command
#[derive(Debug, Clone, PartialEq)]
pub struct SetParams {
    pub key: String,
    pub value: Vec<u8>,
}

impl SetParams {
    /// Parse SET command parameters from RESP array items
    fn parse(items: &[Value]) -> Option<Self> {
        if items.len() != 3 {
            return None;
        }

        let key = match &items[1] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
            Value::SimpleString(s) => s.clone(),
            _ => return None,
        };

        let value = match &items[2] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return None,
        };

        Some(SetParams { key, value })
    }
}

/// SET command executor
pub struct SetCmd;

#[async_trait]
impl Command for SetCmd {
    async fn execute(&self, items: &[Value], store: &Store) -> Value {
        let params = match SetParams::parse(items) {
            Some(params) => params,
            None => return Value::error("ERR wrong number of arguments for 'set' command"),
        };

        match store.set(params.key, params.value) {
            Ok(_) => Value::ok(),
            Err(e) => Value::error(format!("ERR {}", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    #[test]
    fn test_set_params_parse_success() {
        let items = vec![
            Value::BulkString(Some(b"SET".to_vec())),
            Value::BulkString(Some(b"mykey".to_vec())),
            Value::BulkString(Some(b"myvalue".to_vec())),
        ];
        let params = SetParams::parse(&items).unwrap();
        assert_eq!(params.key, "mykey");
        assert_eq!(params.value, b"myvalue");
    }

    #[test]
    fn test_set_params_parse_wrong_args() {
        let items = vec![
            Value::BulkString(Some(b"SET".to_vec())),
            Value::BulkString(Some(b"key".to_vec())),
        ];
        assert!(SetParams::parse(&items).is_none());
    }

    #[tokio::test]
    async fn test_set_cmd_execute_success() {
        let store = Store::new();
        let items = vec![
            Value::BulkString(Some(b"SET".to_vec())),
            Value::BulkString(Some(b"key".to_vec())),
            Value::BulkString(Some(b"value".to_vec())),
        ];
        let cmd = SetCmd;
        let result = cmd.execute(&items, &store).await;

        assert_eq!(result, Value::ok());
        assert_eq!(store.get("key").unwrap(), Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_set_cmd_execute_wrong_args() {
        let store = Store::new();
        let items = vec![
            Value::BulkString(Some(b"SET".to_vec())),
            Value::BulkString(Some(b"key".to_vec())),
        ];
        let cmd = SetCmd;
        let result = cmd.execute(&items, &store).await;

        assert_eq!(
            result,
            Value::error("ERR wrong number of arguments for 'set' command")
        );
    }
}
