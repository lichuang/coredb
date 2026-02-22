use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::store::Store;

/// GET command: GET key
#[derive(Debug, Clone, PartialEq)]
pub struct GetCmd {
    pub key: String,
}

impl GetCmd {
    /// Create a new GET command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    /// Parse GET command from RESP array items
    pub fn parse(items: &[Value]) -> Option<Command> {
        if items.len() != 2 {
            return Some(Command::Unknown(
                "ERR wrong number of arguments for 'get' command".to_string(),
            ));
        }

        let key = match &items[1] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
            Value::SimpleString(s) => s.clone(),
            _ => {
                return Some(Command::Unknown(
                    "ERR invalid key argument".to_string(),
                ))
            }
        };

        Some(Command::Get(GetCmd::new(key)))
    }

    /// Execute the GET command
    pub fn execute(&self, store: &Store) -> Value {
        match store.get(&self.key) {
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
    fn test_get_cmd_struct_creation() {
        let cmd = GetCmd::new("mykey");
        assert_eq!(cmd.key, "mykey");
    }

    #[test]
    fn test_get_cmd_execute() {
        let store = Store::new();
        
        // First set a value
        let _ = store.set("testkey".to_string(), b"testvalue".to_vec());
        
        // Then get it
        let get_cmd = GetCmd::new("testkey");
        let result = get_cmd.execute(&store);
        
        assert_eq!(result, Value::BulkString(Some(b"testvalue".to_vec())));
    }

    #[test]
    fn test_get_cmd_execute_not_found() {
        let store = Store::new();
        let get_cmd = GetCmd::new("nonexistent");
        let result = get_cmd.execute(&store);
        
        assert_eq!(result, Value::BulkString(None));
    }
}
