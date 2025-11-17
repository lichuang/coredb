use clap::Parser;
use tokio::signal;

// mod base;
mod config;
// mod engine_traits;
mod protocol;
mod raft;
// mod rocksdb_engine;
mod log_wrappers;
mod server;
// mod storage;
mod errors;
mod util;

use config::Config;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
  #[arg(short, long)]
  pub config: Option<String>,
}

#[tokio::main]
async fn main() -> errors::Result<()> {
  let args = Args::parse();
  // println!("args: {:?}", args);

  let config = Config::new(&args.config)?;
  // println!("config: {:?}", config);

  let subscriber = fmt::Subscriber::builder()
    .with_max_level(config.log_level)
    .with_ansi(true)
    //.with_timer(fmt::time::UtcTime::rfc_3339())
    .with_span_events(FmtSpan::CLOSE)
    .finish();
  tracing::subscriber::set_global_default(subscriber).unwrap();

  server::run(config, signal::ctrl_c()).await
}
