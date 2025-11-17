use std::collections::HashMap;

use configparser::ini::Ini;
use configparser::ini::IniDefault;
use tracing::Level;

use crate::errors::Result;
use crate::raft::NodeId;

#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
  #[error("Parse int error: {0}")]
  ParseInt(#[from] std::num::ParseIntError),

  #[error("Parse log level error: {0}")]
  ParseLevel(#[from] tracing::metadata::ParseLevelError),

  #[error("Convert error: {0}")]
  Infallible(#[from] std::convert::Infallible),
}

#[derive(Debug)]
pub struct Config {
  pub server_host: String,
  pub server_port: u32,

  pub raft_host: String,
  pub raft_port: u32,

  pub node_id: NodeId,

  pub log_level: Level,

  pub data_dir: String,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      server_host: "127.0.0.1".to_string(),
      server_port: 6622,

      raft_host: "127.0.0.1".to_string(),
      raft_port: 22866,

      node_id: 1,
      log_level: Level::INFO,
      data_dir: "./.coredb_data/".to_string(),
    }
  }
}

impl Config {
  pub fn new(config_file: &Option<String>) -> Result<Self, ConfigError> {
    let mut config = Self::default();

    if let Some(config_file) = config_file {
      let mut default = IniDefault::default();
      // default.comment_symbols = vec![';'];
      default.delimiters = vec![' '];

      if let Some(map) = Ini::new_from_defaults(default)
        .load(config_file)
        .unwrap()
        .get("default")
      {
        let config_map: HashMap<_, _> = map
          .iter()
          .into_iter()
          .filter_map(|(k, v)| v.clone().map(|val| (k, val)))
          .collect();

        for (key, value) in config_map.into_iter() {
          if *key == "server_host" {
            config.server_host = value;
          } else if *key == "server_port" {
            config.server_port = value.parse()?;
          } else if *key == "log_level" {
            config.log_level = value.parse()?;
          } else if *key == "data_dir" {
            config.data_dir = value.parse()?;
          }
        }
      }
    }

    Ok(config)
  }
}
