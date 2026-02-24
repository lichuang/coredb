use crate::encoding::StringValue;
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
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

/// Get a value from the server, checking for expiration.
/// Returns (value, expired) where `expired` is true if the key was expired and deleted.
async fn get_value_check_expiry(
    server: &Server,
    key: &str,
) -> Result<Option<Vec<u8>>, String> {
    let raw_value = match server.get(key).await? {
        Some(v) => v,
        None => return Ok(None),
    };

    // Deserialize and check expiration
    let string_value = match StringValue::deserialize(&raw_value) {
        Ok(v) => v,
        Err(_) => return Err("corrupted value".to_string()),
    };

    // Check if expired
    if string_value.is_expired(now_ms()) {
        // Lazily delete the expired key
        let _ = server.delete(key).await;
        return Ok(None);
    }

    Ok(Some(string_value.data))
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

        match get_value_check_expiry(server, &params.key).await {
            Ok(Some(data)) => Value::BulkString(Some(data)),
            Ok(None) => Value::BulkString(None), // Key not found or expired
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
