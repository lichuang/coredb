use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
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
pub struct GetCommand;

#[async_trait]
impl Command for GetCommand {
    async fn execute(&self, items: &[Value], server: &Server) -> Value {
        let params = match GetParams::parse(items) {
            Some(params) => params,
            None => return Value::error("ERR wrong number of arguments for 'get' command"),
        };

        match server.get(&params.key).await {
            Ok(Some(value)) => Value::BulkString(Some(value)),
            Ok(None) => Value::BulkString(None), // Null bulk string for key not found
            Err(e) => Value::error(format!("ERR {}", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
