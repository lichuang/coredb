use crate::protocol::resp::Value;
use crate::store::Store;
use async_trait::async_trait;
use std::collections::HashMap;

/// Command trait that all Redis commands must implement
#[async_trait]
pub trait Command: Send + Sync {
    /// Execute the command with given RESP items and store
    async fn execute(&self, items: &[Value], store: &Store) -> Value;
}

/// Command factory for creating and executing commands
pub struct CommandFactory {
    commands: HashMap<String, Box<dyn Command>>,
}

impl CommandFactory {
    /// Create a new empty command factory
    pub fn new() -> Self {
        Self {
            commands: HashMap::new(),
        }
    }

    /// Register a command with given name
    pub fn register<C: Command + 'static>(&mut self, name: impl Into<String>, cmd: C) {
        self.commands.insert(name.into(), Box::new(cmd));
    }

    /// Execute a RESP command on the given store
    pub async fn execute(&self, value: Value, store: &Store) -> Value {
        match value {
            Value::Array(Some(items)) if !items.is_empty() => {
                // Extract command name
                let cmd_name = match &items[0] {
                    Value::BulkString(Some(data)) => {
                        String::from_utf8_lossy(data).to_uppercase()
                    }
                    Value::SimpleString(s) => s.to_uppercase(),
                    _ => return Value::error("invalid command format"),
                };

                // Find and execute command
                match self.commands.get(&cmd_name) {
                    Some(cmd) => cmd.execute(&items, store).await,
                    None => Value::error(format!("unknown command '{}'", cmd_name)),
                }
            }
            _ => Value::error("ERR failed to parse command"),
        }
    }
}

impl Default for CommandFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::get::GetCmd;
    use crate::protocol::set::SetCmd;
    use crate::store::Store;

    fn create_factory() -> CommandFactory {
        let mut factory = CommandFactory::new();
        factory.register("GET", GetCmd);
        factory.register("SET", SetCmd);
        factory
    }

    #[tokio::test]
    async fn test_factory_execute_get_not_found() {
        let factory = create_factory();
        let store = Store::new();
        let resp = Value::Array(Some(vec![
            Value::BulkString(Some(b"GET".to_vec())),
            Value::BulkString(Some(b"nonexistent".to_vec())),
        ]));

        let result = factory.execute(resp, &store).await;
        assert_eq!(result, Value::BulkString(None));
    }

    #[tokio::test]
    async fn test_factory_execute_set_and_get() {
        let factory = create_factory();
        let store = Store::new();

        // SET
        let set_resp = Value::Array(Some(vec![
            Value::BulkString(Some(b"SET".to_vec())),
            Value::BulkString(Some(b"mykey".to_vec())),
            Value::BulkString(Some(b"myvalue".to_vec())),
        ]));
        let set_result = factory.execute(set_resp, &store).await;
        assert_eq!(set_result, Value::ok());

        // GET
        let get_resp = Value::Array(Some(vec![
            Value::BulkString(Some(b"GET".to_vec())),
            Value::BulkString(Some(b"mykey".to_vec())),
        ]));
        let get_result = factory.execute(get_resp, &store).await;
        assert_eq!(get_result, Value::BulkString(Some(b"myvalue".to_vec())));
    }

    #[tokio::test]
    async fn test_factory_execute_unknown_command() {
        let factory = create_factory();
        let store = Store::new();
        let resp = Value::Array(Some(vec![
            Value::BulkString(Some(b"UNKNOWN".to_vec())),
        ]));

        let result = factory.execute(resp, &store).await;
        assert_eq!(result, Value::error("unknown command 'UNKNOWN'"));
    }

    #[tokio::test]
    async fn test_factory_execute_parse_error() {
        let factory = create_factory();
        let store = Store::new();
        let resp = Value::SimpleString("not a command".to_string());

        let result = factory.execute(resp, &store).await;
        assert_eq!(result, Value::error("ERR failed to parse command"));
    }
}
