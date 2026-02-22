use crate::protocol::get::GetCmd;
use crate::protocol::resp::Value;
use crate::protocol::set::SetCmd;
use crate::store::Store;

/// Redis command types
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// GET key
    Get(GetCmd),
    /// SET key value
    Set(SetCmd),
    /// Unknown or unsupported command
    Unknown(String),
}

impl Command {
    /// Parse a RESP array into a Command
    fn from_resp(value: Value) -> Option<Self> {
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
                    "GET" => GetCmd::parse(&items),
                    "SET" => SetCmd::parse(&items),
                    _ => Some(Command::Unknown(format!("unknown command '{}'", cmd_name))),
                }
            }
            _ => None,
        }
    }

    /// Execute the command on the given store and return the response
    fn execute_internal(&self, store: &Store) -> Value {
        match self {
            Command::Get(cmd) => cmd.execute(store),
            Command::Set(cmd) => cmd.execute(store),
            Command::Unknown(msg) => Value::error(msg.clone()),
        }
    }

    /// Parse and execute a RESP command on the given store
    pub fn execute(value: Value, store: &Store) -> Value {
        match Self::from_resp(value) {
            Some(cmd) => cmd.execute_internal(store),
            None => Value::error("ERR failed to parse command"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    #[test]
    fn test_parse_get_command() {
        let resp = Value::Array(Some(vec![
            Value::BulkString(Some(b"GET".to_vec())),
            Value::BulkString(Some(b"mykey".to_vec())),
        ]));

        let cmd = Command::from_resp(resp).unwrap();
        match cmd {
            Command::Get(get_cmd) => {
                assert_eq!(get_cmd.key, "mykey");
            }
            _ => panic!("Expected GET command"),
        }
    }

    #[test]
    fn test_parse_set_command() {
        let resp = Value::Array(Some(vec![
            Value::BulkString(Some(b"SET".to_vec())),
            Value::BulkString(Some(b"mykey".to_vec())),
            Value::BulkString(Some(b"myvalue".to_vec())),
        ]));

        let cmd = Command::from_resp(resp).unwrap();
        match cmd {
            Command::Set(set_cmd) => {
                assert_eq!(set_cmd.key, "mykey");
                assert_eq!(set_cmd.value, b"myvalue");
            }
            _ => panic!("Expected SET command"),
        }
    }

    #[test]
    fn test_execute_get_not_found() {
        let store = Store::new();
        let resp = Value::Array(Some(vec![
            Value::BulkString(Some(b"GET".to_vec())),
            Value::BulkString(Some(b"nonexistent".to_vec())),
        ]));

        let result = Command::execute(resp, &store);
        
        assert_eq!(result, Value::BulkString(None));
    }

    #[test]
    fn test_execute_set_and_get() {
        let store = Store::new();
        
        // SET
        let set_resp = Value::Array(Some(vec![
            Value::BulkString(Some(b"SET".to_vec())),
            Value::BulkString(Some(b"mykey".to_vec())),
            Value::BulkString(Some(b"myvalue".to_vec())),
        ]));
        let set_result = Command::execute(set_resp, &store);
        assert_eq!(set_result, Value::ok());
        
        // GET
        let get_resp = Value::Array(Some(vec![
            Value::BulkString(Some(b"GET".to_vec())),
            Value::BulkString(Some(b"mykey".to_vec())),
        ]));
        let get_result = Command::execute(get_resp, &store);
        assert_eq!(get_result, Value::BulkString(Some(b"myvalue".to_vec())));
    }

    #[test]
    fn test_execute_invalid_command() {
        let store = Store::new();
        let resp = Value::Array(Some(vec![
            Value::BulkString(Some(b"UNKNOWN".to_vec())),
        ]));

        let result = Command::execute(resp, &store);
        
        assert_eq!(result, Value::error("unknown command 'UNKNOWN'"));
    }

    #[test]
    fn test_execute_parse_error() {
        let store = Store::new();
        // Invalid RESP (not an array)
        let resp = Value::SimpleString("not a command".to_string());

        let result = Command::execute(resp, &store);
        
        assert_eq!(result, Value::error("ERR failed to parse command"));
    }
}
