use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::store::Store;

/// SET command: SET key value
#[derive(Debug, Clone, PartialEq)]
pub struct SetCmd {
    pub key: String,
    pub value: Vec<u8>,
}

impl SetCmd {
    /// Create a new SET command
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Parse SET command from RESP array items
    pub fn parse(items: &[Value]) -> Option<Command> {
        if items.len() != 3 {
            return Some(Command::Unknown(
                "ERR wrong number of arguments for 'set' command".to_string(),
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

        let value = match &items[2] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => {
                return Some(Command::Unknown(
                    "ERR invalid value argument".to_string(),
                ))
            }
        };

        Some(Command::Set(SetCmd::new(key, value)))
    }

    /// Execute the SET command
    pub fn execute(&self, store: &Store) -> Value {
        match store.set(self.key.clone(), self.value.clone()) {
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
    fn test_set_cmd_struct_creation() {
        let cmd = SetCmd::new("mykey", "myvalue");
        assert_eq!(cmd.key, "mykey");
        assert_eq!(cmd.value, b"myvalue");
    }

    #[test]
    fn test_set_cmd_execute() {
        let store = Store::new();
        let set_cmd = SetCmd::new("key", b"value");
        let result = set_cmd.execute(&store);
        
        assert_eq!(result, Value::ok());
        
        // Verify the value was set
        assert_eq!(store.get("key").unwrap(), Some(b"value".to_vec()));
    }
}
