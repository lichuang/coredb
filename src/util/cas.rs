//! Conditional-write (compare-and-set) retry policy.
//!
//! Contending CAS writers re-read and re-submit in a loop; a tight loop would
//! just re-compete in lockstep, so retries back off exponentially with jitter
//! to de-synchronize the losers.

use std::time::Duration;

/// Bound on compare-and-set retries before a conditional write gives up and
/// surfaces an error to the client.
pub const MAX_CAS_RETRIES: usize = 128;

const BASE_DELAY_MS: u64 = 1;
const MAX_DELAY_MS: u64 = 50;

/// Backoff for the Nth failed CAS attempt, capped at [`MAX_DELAY_MS`].
/// Exponential in the attempt, with clock-derived jitter so concurrent losers
/// spread out instead of re-competing in lockstep.
pub fn backoff_delay(attempt: usize) -> Duration {
  let jitter_ms = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .map(|d| (d.subsec_nanos() as u64 ^ (d.as_secs() & 0xFFFF)) % (BASE_DELAY_MS + 1))
    .unwrap_or(0);
  let exponential = BASE_DELAY_MS << attempt.min(6);
  Duration::from_millis((exponential + jitter_ms).min(MAX_DELAY_MS))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_max_cas_retries_is_bounded() {
    // A runaway retry loop would hang the server thread instead of reporting
    // an error to the client, so the bound must be small and finite.
    assert!(MAX_CAS_RETRIES > 0);
    assert!(MAX_CAS_RETRIES <= 1000);
  }

  #[test]
  fn test_backoff_delay_is_bounded() {
    for attempt in 0..40 {
      let d = backoff_delay(attempt);
      assert!(
        d.as_millis() <= u128::from(MAX_DELAY_MS),
        "attempt {attempt}: {d:?}"
      );
    }
  }
}
