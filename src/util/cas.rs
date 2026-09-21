//! Conditional-write (compare-and-set) retry policy.

/// Bound on compare-and-set retries before a conditional write gives up and
/// surfaces an error to the client.
pub const MAX_CAS_RETRIES: usize = 32;
