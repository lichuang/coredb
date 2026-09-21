//! KEYS command implementation
//!
//! KEYS pattern
//! Returns all key names that match the given glob-style pattern.
//!
//! Supported glob-style patterns:
//! - `h?llo` matches hello, hallo, hxllo
//! - `h*llo` matches hllo, heeeello
//! - `h[ae]llo` matches hello, hallo
//! - `h[^e]llo` matches hallo, hbllo, but not hello
//! - `h[a-b]llo` matches hallo, hbllo
//! - `\*` matches a literal asterisk (escape)

use crate::encoding::hash::HashFieldValue;
use crate::encoding::is_expired;
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// KEYS command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct KeysParams {
  pub pattern: String,
}

impl KeysParams {
  /// Parse KEYS command parameters from RESP array items
  /// Format: KEYS pattern
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 2 {
      return Err(ProtocolError::WrongArgCount("keys"));
    }

    let pattern = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::WrongArgCount("keys")),
    };

    Ok(KeysParams { pattern })
  }
}

/// Check if a storage key is an internal sub-key (hex-encoded key_len|key|version|sub_key_part).
///
/// User-facing keys are stored as raw bytes, while internal sub-keys for complex types
/// (hash, list, set, zset, bitmap, etc.) are stored as hex-encoded binary data following
/// the format: key_len(4 bytes BE) | key | version(8 bytes BE) | sub_key_part
fn is_sub_key(key_bytes: &[u8]) -> bool {
  // Sub-keys are hex-encoded, so they must have even length and contain only hex chars
  if key_bytes.is_empty() || !key_bytes.len().is_multiple_of(2) {
    return false;
  }

  // Quick check: all bytes must be ASCII hex characters (0-9, a-f)
  for &b in key_bytes {
    if !b.is_ascii_hexdigit() || (b'A'..=b'F').contains(&b) {
      // Redis stores hex as lowercase; uppercase hex chars indicate a user key
      return false;
    }
  }

  // Try to hex-decode and parse as sub-key structure
  let decoded = match hex::decode(key_bytes) {
    Ok(d) => d,
    Err(_) => return false,
  };

  // Use the common sub-key parser from HashFieldValue (same format across all types)
  HashFieldValue::parse_sub_key(&decoded).is_some()
}

/// Glob-style pattern matching, compatible with Redis KEYS patterns.
///
/// Supports:
/// - `*` matches any sequence of characters (including empty)
/// - `?` matches any single character
/// - `[abc]` matches one character from the set
/// - `[a-z]` matches one character in a range
/// - `[^abc]` or `[!abc]` matches one character NOT in the set
/// - `\` escapes the next special character
fn glob_match(pattern: &str, text: &str) -> bool {
  let p: Vec<char> = pattern.chars().collect();
  let t: Vec<char> = text.chars().collect();
  glob_match_impl(&p, 0, &t, 0)
}

fn glob_match_impl(pattern: &[char], pi: usize, text: &[char], ti: usize) -> bool {
  let mut pi = pi;
  let mut ti = ti;

  loop {
    if pi == pattern.len() {
      return ti == text.len();
    }

    let pc = pattern[pi];

    if pc == '\\' && pi + 1 < pattern.len() {
      // Escaped character: match literally
      pi += 1;
      if ti >= text.len() || pattern[pi] != text[ti] {
        return false;
      }
      ti += 1;
      pi += 1;
      continue;
    }

    match pc {
      '*' => {
        // Skip consecutive stars
        pi += 1;
        while pi < pattern.len() && pattern[pi] == '*' {
          pi += 1;
        }
        // If star is at end of pattern, match everything
        if pi == pattern.len() {
          return true;
        }
        // Try matching star with 0..N characters
        for k in ti..=text.len() {
          if glob_match_impl(pattern, pi, text, k) {
            return true;
          }
        }
        return false;
      }
      '?' => {
        if ti >= text.len() {
          return false;
        }
        pi += 1;
        ti += 1;
      }
      '[' => {
        if ti >= text.len() {
          return false;
        }
        let (matched, new_pi) = match_charset(pattern, pi, text[ti]);
        if !matched {
          return false;
        }
        pi = new_pi;
        ti += 1;
      }
      _ => {
        if ti >= text.len() || pc != text[ti] {
          return false;
        }
        pi += 1;
        ti += 1;
      }
    }
  }
}

/// Match a character against a bracket expression like [abc], [a-z], [^abc]
///
/// Returns (matched, index_after_closing_bracket)
fn match_charset(pattern: &[char], pi: usize, c: char) -> (bool, usize) {
  let mut idx = pi + 1; // skip '['

  // Check for negation
  let negate = if idx < pattern.len() && (pattern[idx] == '^' || pattern[idx] == '!') {
    idx += 1;
    true
  } else {
    false
  };

  let mut matched = false;

  while idx < pattern.len() && pattern[idx] != ']' {
    if idx + 2 < pattern.len() && pattern[idx + 1] == '-' && pattern[idx + 2] != ']' {
      // Range: a-z
      let start = pattern[idx];
      let end = pattern[idx + 2];
      if c >= start && c <= end {
        matched = true;
      }
      idx += 3;
    } else {
      // Single character
      if pattern[idx] == c {
        matched = true;
      }
      idx += 1;
    }
  }

  // Skip past closing ']'
  if idx < pattern.len() {
    idx += 1;
  }

  (matched != negate, idx)
}

/// KEYS command executor
pub struct KeysCommand;

#[async_trait]
impl Command for KeysCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = KeysParams::parse(items)?;

    // Scan all keys using empty prefix
    let all_entries = server.scan_prefix(&[]).await?;

    let now = now_ms();
    let mut result_keys: Vec<Value> = Vec::new();

    for (key_bytes, value_bytes) in &all_entries {
      // Skip internal sub-keys (hex-encoded key_len|key|version|sub_key)
      if is_sub_key(key_bytes) {
        continue;
      }

      // Convert key to string for pattern matching
      let key_str = String::from_utf8_lossy(key_bytes).to_string();

      // Apply glob pattern matching
      if !glob_match(&params.pattern, &key_str) {
        continue;
      }

      // Check expiration: try to deserialize value and check if expired
      if is_expired(value_bytes, now) {
        continue;
      }

      result_keys.push(Value::BulkString(Some(key_bytes.clone())));
    }

    Ok(Value::Array(Some(result_keys)))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::encoding::{ListElementValue, SetMemberValue};

  // ==================== KeysParams Tests ====================

  #[test]
  fn test_keys_params_parse_success() {
    let items = vec![
      Value::BulkString(Some(b"KEYS".to_vec())),
      Value::BulkString(Some(b"*".to_vec())),
    ];
    let params = KeysParams::parse(&items).unwrap();
    assert_eq!(params.pattern, "*");
  }

  #[test]
  fn test_keys_params_parse_pattern_with_wildcards() {
    let items = vec![
      Value::BulkString(Some(b"KEYS".to_vec())),
      Value::BulkString(Some(b"mykey:*".to_vec())),
    ];
    let params = KeysParams::parse(&items).unwrap();
    assert_eq!(params.pattern, "mykey:*");
  }

  #[test]
  fn test_keys_params_parse_no_args() {
    let items = vec![Value::BulkString(Some(b"KEYS".to_vec()))];
    assert!(KeysParams::parse(&items).is_err());
  }

  #[test]
  fn test_keys_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"KEYS".to_vec())),
      Value::BulkString(Some(b"*".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(KeysParams::parse(&items).is_err());
  }

  #[test]
  fn test_keys_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("KEYS".to_string()),
      Value::SimpleString("*".to_string()),
    ];
    let params = KeysParams::parse(&items).unwrap();
    assert_eq!(params.pattern, "*");
  }

  // ==================== Glob Match Tests ====================

  #[test]
  fn test_glob_star_matches_everything() {
    assert!(glob_match("*", ""));
    assert!(glob_match("*", "anything"));
    assert!(glob_match("*", "hello world"));
  }

  #[test]
  fn test_glob_star_prefix() {
    assert!(glob_match("h*", "hello"));
    assert!(glob_match("h*", "h"));
    assert!(glob_match("h*", "hxyz"));
    assert!(!glob_match("h*", "xhello"));
  }

  #[test]
  fn test_glob_star_suffix() {
    assert!(glob_match("*llo", "hello"));
    assert!(glob_match("*llo", "llo"));
    assert!(!glob_match("*llo", "hel"));
  }

  #[test]
  fn test_glob_star_middle() {
    assert!(glob_match("h*llo", "hello"));
    assert!(glob_match("h*llo", "hllo"));
    assert!(glob_match("h*llo", "heeeello"));
    assert!(!glob_match("h*llo", "heo"));
  }

  #[test]
  fn test_glob_question() {
    assert!(glob_match("h?llo", "hello"));
    assert!(glob_match("h?llo", "hallo"));
    assert!(glob_match("h?llo", "hxllo"));
    assert!(!glob_match("h?llo", "hllo"));
    assert!(!glob_match("h?llo", "heello"));
  }

  #[test]
  fn test_glob_charset_positive() {
    assert!(glob_match("h[ae]llo", "hello"));
    assert!(glob_match("h[ae]llo", "hallo"));
    assert!(!glob_match("h[ae]llo", "hillo"));
  }

  #[test]
  fn test_glob_charset_range() {
    assert!(glob_match("h[a-b]llo", "hallo"));
    assert!(glob_match("h[a-b]llo", "hbllo"));
    assert!(!glob_match("h[a-b]llo", "hcllo"));
  }

  #[test]
  fn test_glob_charset_negate_caret() {
    assert!(glob_match("h[^e]llo", "hallo"));
    assert!(glob_match("h[^e]llo", "hbllo"));
    assert!(!glob_match("h[^e]llo", "hello"));
  }

  #[test]
  fn test_glob_charset_negate_bang() {
    assert!(glob_match("h[!e]llo", "hallo"));
    assert!(!glob_match("h[!e]llo", "hello"));
  }

  #[test]
  fn test_glob_escape() {
    assert!(glob_match("\\*", "*"));
    assert!(!glob_match("\\*", "hello"));
    assert!(glob_match("\\?", "?"));
    assert!(!glob_match("\\?", "a"));
  }

  #[test]
  fn test_glob_exact_match() {
    assert!(glob_match("hello", "hello"));
    assert!(!glob_match("hello", "world"));
    assert!(!glob_match("hello", "helloo"));
  }

  #[test]
  fn test_glob_empty_pattern() {
    assert!(glob_match("", ""));
    assert!(!glob_match("", "a"));
  }

  #[test]
  fn test_glob_multiple_stars() {
    assert!(glob_match("h**llo", "hello"));
    assert!(glob_match("**", "anything"));
  }

  #[test]
  fn test_glob_complex_patterns() {
    assert!(glob_match("user:*:session", "user:123:session"));
    assert!(glob_match("user:*:session", "user::session"));
    assert!(!glob_match("user:*:session", "user:123:other"));
    assert!(glob_match("cache:[0-9]*", "cache:123abc"));
    assert!(!glob_match("cache:[0-9]*", "cache:abc"));
  }

  // ==================== Sub-key Detection Tests ====================

  #[test]
  fn test_is_sub_key_rejects_empty() {
    assert!(!is_sub_key(b""));
  }

  #[test]
  fn test_is_sub_key_rejects_odd_length() {
    assert!(!is_sub_key(b"abc"));
  }

  #[test]
  fn test_is_sub_key_rejects_user_key() {
    assert!(!is_sub_key(b"mykey"));
    assert!(!is_sub_key(b"hello"));
    assert!(!is_sub_key(b"user:123"));
  }

  #[test]
  fn test_is_sub_key_rejects_uppercase_hex() {
    // User key that happens to be uppercase hex
    assert!(!is_sub_key(b"ABCDEF"));
  }

  #[test]
  fn test_is_sub_key_detects_valid_sub_key() {
    // Build a real sub-key and verify detection
    let sub_key_hex = HashFieldValue::build_sub_key_hex(b"myhash", 12345, b"field1");
    assert!(is_sub_key(sub_key_hex.as_bytes()));
  }

  #[test]
  fn test_is_sub_key_detects_list_sub_key() {
    let sub_key_hex = ListElementValue::build_sub_key_hex(b"mylist", 99999, 0);
    assert!(is_sub_key(sub_key_hex.as_bytes()));
  }

  #[test]
  fn test_is_sub_key_detects_set_sub_key() {
    let sub_key_hex = SetMemberValue::build_sub_key_hex(b"myset", 42, b"member1");
    assert!(is_sub_key(sub_key_hex.as_bytes()));
  }

  // ==================== match_charset Tests ====================

  #[test]
  fn test_match_charset_single() {
    let p: Vec<char> = "[ae]x".chars().collect();
    assert_eq!(match_charset(&p, 0, 'a'), (true, 4));
    assert_eq!(match_charset(&p, 0, 'e'), (true, 4));
    assert_eq!(match_charset(&p, 0, 'b'), (false, 4));
  }

  #[test]
  fn test_match_charset_range() {
    let p: Vec<char> = "[a-c]x".chars().collect();
    assert_eq!(match_charset(&p, 0, 'a'), (true, 5));
    assert_eq!(match_charset(&p, 0, 'b'), (true, 5));
    assert_eq!(match_charset(&p, 0, 'c'), (true, 5));
    assert_eq!(match_charset(&p, 0, 'd'), (false, 5));
  }

  #[test]
  fn test_match_charset_negate() {
    let p: Vec<char> = "[^a]x".chars().collect();
    assert_eq!(match_charset(&p, 0, 'a'), (false, 4));
    assert_eq!(match_charset(&p, 0, 'b'), (true, 4));
  }
}
