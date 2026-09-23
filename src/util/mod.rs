//! Utility functions

pub mod cas;
pub mod time;

pub use cas::{MAX_CAS_RETRIES, backoff_delay};
pub use time::now_ms;
