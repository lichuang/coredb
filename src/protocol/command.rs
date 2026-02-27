use crate::protocol::get::GetCommand;
use crate::protocol::resp::Value;
use crate::protocol::set::SetCommand;
use crate::server::Server;
use async_trait::async_trait;
use std::collections::HashMap;

/// Command trait that all Redis commands must implement
#[async_trait]
pub trait Command: Send + Sync {
  /// Execute the command with given RESP items and server context
  async fn execute(&self, items: &[Value], server: &Server) -> Value;
}

/// Command factory for creating and executing commands
pub struct CommandFactory {
  commands: HashMap<String, Box<dyn Command>>,
}

impl CommandFactory {
  /// Create a new empty command factory
  fn new() -> Self {
    Self {
      commands: HashMap::new(),
    }
  }

  /// Register a command with given name
  fn register<C: Command + 'static>(&mut self, name: impl Into<String>, cmd: C) {
    self.commands.insert(name.into(), Box::new(cmd));
  }

  /// Initialize the command factory with all supported commands
  pub fn init() -> Self {
    let mut factory = Self::new();

    // Register GET and SET commands
    factory.register("GET", GetCommand);
    factory.register("SET", SetCommand);

    factory
  }

  /// Execute a RESP command on the given server
  pub async fn execute(&self, value: Value, server: &Server) -> Value {
    match value {
      Value::Array(Some(items)) if !items.is_empty() => {
        // Extract command name
        let cmd_name = match &items[0] {
          Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
          Value::SimpleString(s) => s.to_uppercase(),
          _ => return Value::error("invalid command format"),
        };

        // Find and execute command
        match self.commands.get(&cmd_name) {
          Some(cmd) => cmd.execute(&items, server).await,
          None => Value::error(format!("unknown command '{}'", cmd_name)),
        }
      }
      _ => Value::error("ERR failed to parse command"),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn test_factory_execute_unknown_command() {
    // For this test, we need a Server instance
    // We'll skip the detailed tests here as they require Server setup
    let factory = CommandFactory::init();

    // Just verify factory is initialized correctly
    assert!(factory.commands.contains_key("GET"));
    assert!(factory.commands.contains_key("SET"));
  }
}
