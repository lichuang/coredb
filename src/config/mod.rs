use rockraft::config::Config as RockraftConfig;
use serde::{Deserialize, Serialize};
use std::fs;

/// Log configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LogConfig {
  /// Log file path, if not set, logs will be printed to stdout
  pub file: Option<String>,
  /// Log level, default is "info"
  #[serde(default = "default_log_level")]
  pub level: String,
}

fn default_log_level() -> String {
  "info".to_string()
}

impl Default for LogConfig {
  fn default() -> Self {
    Self {
      file: None,
      level: default_log_level(),
    }
  }
}

/// CoreDB configuration with Raft support
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
  #[serde(flatten)]
  pub raft: RockraftConfig,

  /// Server listening address (Redis protocol)
  #[serde(default = "default_server_addr")]
  pub server_addr: String,

  /// Log configuration
  #[serde(default)]
  pub log: LogConfig,
}

fn default_server_addr() -> String {
  "0.0.0.0:6379".to_string()
}

impl Config {
  /// Load configuration from TOML file
  pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
    let config_str = fs::read_to_string(path)
      .map_err(|e| format!("Failed to read config file '{}': {}", path, e))?;

    let config: Config = toml::from_str(&config_str)
      .map_err(|e| format!("Failed to parse config file '{}': {}", path, e))?;

    // Validate rockraft config
    config.raft.validate()?;

    Ok(config)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_default_config() {
    let config_str = r#"
node_id = 1
server_addr = "0.0.0.0:6379"

[raft]
address = "127.0.0.1:7001"
single = true
join = []

[rocksdb]
data_path = "/tmp/coredb/node1"
max_open_files = 10000
"#;

    let config: Config = toml::from_str(config_str).unwrap();
    assert_eq!(config.raft.node_id, 1);
    assert_eq!(config.server_addr, "0.0.0.0:6379");
    assert_eq!(config.raft.raft.address, "127.0.0.1:7001");
    assert!(config.raft.raft.single);
  }
}
