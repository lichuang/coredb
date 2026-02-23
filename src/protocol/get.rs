use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::store::Store;
use async_trait::async_trait;

/// Parameters for GET command
#[derive(Debug, Clone, PartialEq)]
pub struct GetParams {
    pub key: String,
}

impl GetParams {
    /// Parse GET command parameters from RESP array items
    fn parse(items: &[Value]) -> Option<Self> {
        if items.len() != 2 {
            return None;
        }

        let key = match &items[1] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
            Value::SimpleString(s) => s.clone(),
            _ => return None,
        };

        Some(GetParams { key })
    }
}

/// GET command executor
pub struct GetCmd;

#[async_trait]
impl Command for GetCmd {
    async fn execute(&self, items: &[Value], store: &Store) -> Value {
        let params = match GetParams::parse(items) {
            Some(params) => params,
            None => return Value::error("ERR wrong number of arguments for 'get' command"),
        };

        match store.get(&params.key) {
            Ok(Some(value)) => Value::BulkString(Some(value)),
            Ok(None) => Value::BulkString(None), // Null bulk string for key not found
            Err(e) => Value::error(format!("ERR {}", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    #[test]
    fn test_get_params_parse_success() {
        let items = vec![
            Value::BulkString(Some(b"GET".to_vec())),
            Value::BulkString(Some(b"mykey".to_vec())),
        ];
        let params = GetParams::parse(&items).unwrap();
        assert_eq!(params.key, "mykey");
    }

    #[test]
    fn test_get_params_parse_wrong_args() {
        let items = vec![Value::BulkString(Some(b"GET".to_vec()))];
        assert!(GetParams::parse(&items).is_none());
    }

    #[tokio::test]
    async fn test_get_cmd_execute_success() {
        let store = Store::new();
        let _ = store.set("testkey".to_string(), b"testvalue".to_vec());

        let items = vec![
            Value::BulkString(Some(b"GET".to_vec())),
            Value::BulkString(Some(b"testkey".to_vec())),
        ];
        let cmd = GetCmd;
        let result = cmd.execute(&items, &store).await;

        assert_eq!(result, Value::BulkString(Some(b"testvalue".to_vec())));
    }

    #[tokio::test]
    async fn test_get_cmd_execute_not_found() {
        let store = Store::new();
        let items = vec![
            Value::BulkString(Some(b"GET".to_vec())),
            Value::BulkString(Some(b"nonexistent".to_vec())),
        ];
        let cmd = GetCmd;
        let result = cmd.execute(&items, &store).await;

        assert_eq!(result, Value::BulkString(None));
    }

    #[tokio::test]
    async fn test_get_cmd_execute_wrong_args() {
        let store = Store::new();
        let items = vec![Value::BulkString(Some(b"GET".to_vec()))];
        let cmd = GetCmd;
        let result = cmd.execute(&items, &store).await;

        assert_eq!(
            result,
            Value::error("ERR wrong number of arguments for 'get' command")
        );
    }
}
