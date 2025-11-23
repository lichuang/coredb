use super::log_entry::LogEntry;

#[derive(
  serde::Serialize,
  serde::Deserialize,
  Debug,
  Clone,
  PartialEq,
  Eq,
  derive_more::From,
  derive_more::TryInto,
)]
pub enum ForwardRequestBody {
  Ping,

  Write(LogEntry),
}
