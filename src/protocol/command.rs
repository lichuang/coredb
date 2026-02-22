use crate::protocol::resp::Value;
use crate::store::Store;

/// Redis command types
#[derive(Debug)]
pub enum Command {
    /// GET key
    Get { key: String },
    /// SET key value
    Set { key: String, value: Vec<u8> },
    /// Unknown or unsupported command
    Unknown(String),
}

impl Command {
    /// Parse a RESP array into a Command
    pub fn from_resp(value: Value) -> Option<Self> {
        match value {
            Value::Array(Some(items)) if !items.is_empty() => {
                // First item should be the command name
                let cmd_name = match &items[0] {
                    Value::BulkString(Some(data)) => {
                        String::from_utf8_lossy(data).to_uppercase()
                    }
                    Value::SimpleString(s) => s.to_uppercase(),
                    _ => return Some(Command::Unknown("invalid command format".to_string())),
                };

                match cmd_name.as_str() {
                    "GET" => Self::parse_get(&items),
                    "SET" => Self::parse_set(&items),
                    _ => Some(Command::Unknown(format!("unknown command '{}'", cmd_name))),
                }
            }
            _ => None,
        }
    }

    fn parse_get(items: &[Value]) -> Option<Self> {
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

        Some(Command::Get { key })
    }

    fn parse_set(items: &[Value]) -> Option<Self> {
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

        Some(Command::Set { key, value })
    }

    /// Execute the command on the given store and return the response
    pub fn execute(&self, store: &Store) -> Value {
        match self {
            Command::Get { key } => match store.get(key) {
                Ok(Some(value)) => Value::BulkString(Some(value)),
                Ok(None) => Value::BulkString(None), // Null bulk string for key not found
                Err(e) => Value::error(format!("ERR {}", e)),
            },
            Command::Set { key, value } => match store.set(key.clone(), value.clone()) {
                Ok(_) => Value::ok(),
                Err(e) => Value::error(format!("ERR {}", e)),
            },
            Command::Unknown(msg) => Value::error(msg.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_set_command() {
        let resp = Value::Array(Some(vec![
            Value::BulkString(Some(b"SET".to_vec())),
            Value::BulkString(Some(b"mykey".to_vec())),
            Value::BulkString(Some(b"myvalue".to_vec())),
        ]));

        let cmd = Command::from_resp(resp).unwrap();
        match cmd {
            Command::Set { key, value } => {
                assert_eq!(key, "mykey");
                assert_eq!(value, b"myvalue");
            }
            _ => panic!("Expected SET command"),
        }
    }

    #[test]
    fn test_execute_set() {
        let store = Store::new();
        let resp = Value::Array(Some(vec![
            Value::BulkString(Some(b"SET".to_vec())),
            Value::BulkString(Some(b"key".to_vec())),
            Value::BulkString(Some(b"value".to_vec())),
        ]));

        let cmd = Command::from_resp(resp).unwrap();
        let result = cmd.execute(&store);
        
        assert_eq!(result, Value::ok());
    }
}
