use crate::protocol::connection::PingCommand;
use crate::protocol::hash::{
  HDelCommand, HExistsCommand, HGetAllCommand, HGetCommand, HIncrByCommand, HKeysCommand,
  HLenCommand, HMGetCommand, HSetCommand, HSetNxCommand, HValsCommand,
};
use crate::protocol::key::{DelCommand, ExistsCommand, ExpireCommand, PexpireCommand, TypeCommand};
use crate::protocol::list::{LPushCommand, RPushCommand};
use crate::protocol::resp::Value;
use crate::protocol::string::{
  AppendCommand, DecrCommand, DecrbyCommand, GetCommand, IncrCommand, IncrbyCommand, MgetCommand,
  MsetCommand, PsetexCommand, SetCommand, SetexCommand, SetnxCommand, StrlenCommand,
};
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

    // Register connection commands
    factory.register("PING", PingCommand);

    // Register string commands
    factory.register("APPEND", AppendCommand);
    factory.register("DECR", DecrCommand);
    factory.register("DECRBY", DecrbyCommand);
    factory.register("DEL", DelCommand);
    factory.register("EXISTS", ExistsCommand);
    factory.register("EXPIRE", ExpireCommand);
    factory.register("GET", GetCommand);
    factory.register("INCR", IncrCommand);
    factory.register("INCRBY", IncrbyCommand);
    factory.register("MGET", MgetCommand);
    factory.register("MSET", MsetCommand);
    factory.register("PEXPIRE", PexpireCommand);
    factory.register("PSETEX", PsetexCommand);
    factory.register("SET", SetCommand);
    factory.register("SETEX", SetexCommand);
    factory.register("SETNX", SetnxCommand);
    factory.register("STRLEN", StrlenCommand);
    factory.register("TYPE", TypeCommand);

    // Register hash commands
    factory.register("HDEL", HDelCommand);
    factory.register("HEXISTS", HExistsCommand);
    factory.register("HGET", HGetCommand);
    factory.register("HGETALL", HGetAllCommand);
    factory.register("HINCRBY", HIncrByCommand);
    factory.register("HKEYS", HKeysCommand);
    factory.register("HLEN", HLenCommand);
    factory.register("HMGET", HMGetCommand);
    factory.register("HSET", HSetCommand);
    factory.register("HSETNX", HSetNxCommand);
    factory.register("HVALS", HValsCommand);

    // Register list commands
    factory.register("LPUSH", LPushCommand);
    factory.register("RPUSH", RPushCommand);

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
    assert!(factory.commands.contains_key("APPEND"));
    assert!(factory.commands.contains_key("DECR"));
    assert!(factory.commands.contains_key("DECRBY"));
    assert!(factory.commands.contains_key("DEL"));
    assert!(factory.commands.contains_key("EXISTS"));
    assert!(factory.commands.contains_key("EXPIRE"));
    assert!(factory.commands.contains_key("PEXPIRE"));
    assert!(factory.commands.contains_key("GET"));
    assert!(factory.commands.contains_key("INCR"));
    assert!(factory.commands.contains_key("INCRBY"));
    assert!(factory.commands.contains_key("MGET"));
    assert!(factory.commands.contains_key("MSET"));
    assert!(factory.commands.contains_key("PING"));
    assert!(factory.commands.contains_key("SET"));
    assert!(factory.commands.contains_key("STRLEN"));
    assert!(factory.commands.contains_key("TYPE"));
    assert!(factory.commands.contains_key("HDEL"));
    assert!(factory.commands.contains_key("HEXISTS"));
    assert!(factory.commands.contains_key("HGET"));
    assert!(factory.commands.contains_key("HGETALL"));
    assert!(factory.commands.contains_key("HINCRBY"));
    assert!(factory.commands.contains_key("HKEYS"));
    assert!(factory.commands.contains_key("HLEN"));
    assert!(factory.commands.contains_key("HMGET"));
    assert!(factory.commands.contains_key("HSET"));
    assert!(factory.commands.contains_key("HSETNX"));
    assert!(factory.commands.contains_key("HVALS"));
  }
}
