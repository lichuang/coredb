/// RESP (REdis Serialization Protocol) data types
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
  SimpleString(String),
  Error(String),
  Integer(i64),
  BulkString(Option<Vec<u8>>),
  Array(Option<Vec<Value>>),
  /// Key-value mapping (RESP3: %N map, RESP2: flat array *2N)
  Map(Vec<(Value, Value)>),
  /// Ordered pairs, e.g. ZRANGE WITHSCORES (RESP3: array of 2-elem arrays, RESP2: flat array *2N)
  Pairs(Vec<(Value, Value)>),
  /// Boolean (RESP3: #t/#f, RESP2: :1/:0)
  Boolean(bool),
}

impl Value {
  pub fn ok() -> Self {
    Value::SimpleString("OK".to_string())
  }

  pub fn error(msg: impl Into<String>) -> Self {
    Value::Error(msg.into())
  }

  pub fn is_error(&self) -> bool {
    matches!(self, Value::Error(_))
  }

  pub fn encode_proto(&self, proto: u8) -> Vec<u8> {
    let mut buf = Vec::new();
    self.encode_to(proto, &mut buf);
    buf
  }

  #[allow(dead_code)]
  pub fn encode(&self) -> Vec<u8> {
    self.encode_proto(2)
  }

  fn encode_to(&self, proto: u8, buf: &mut Vec<u8>) {
    match self {
      Value::SimpleString(s) => {
        buf.push(b'+');
        buf.extend_from_slice(s.as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Value::Error(e) => {
        buf.push(b'-');
        buf.extend_from_slice(e.as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Value::Integer(i) => {
        buf.push(b':');
        buf.extend_from_slice(i.to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Value::BulkString(None) => {
        if proto == 3 {
          buf.extend_from_slice(b"_\r\n");
        } else {
          buf.extend_from_slice(b"$-1\r\n");
        }
      }
      Value::BulkString(Some(data)) => {
        buf.push(b'$');
        buf.extend_from_slice(data.len().to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(data);
        buf.extend_from_slice(b"\r\n");
      }
      Value::Array(None) => {
        if proto == 3 {
          buf.extend_from_slice(b"_\r\n");
        } else {
          buf.extend_from_slice(b"*-1\r\n");
        }
      }
      Value::Array(Some(items)) => {
        buf.push(b'*');
        buf.extend_from_slice(items.len().to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        for item in items {
          item.encode_to(proto, buf);
        }
      }
      Value::Map(pairs) => {
        if proto == 3 {
          buf.push(b'%');
          buf.extend_from_slice(pairs.len().to_string().as_bytes());
          buf.extend_from_slice(b"\r\n");
        } else {
          buf.push(b'*');
          buf.extend_from_slice((pairs.len() * 2).to_string().as_bytes());
          buf.extend_from_slice(b"\r\n");
        }
        for (k, v) in pairs {
          k.encode_to(proto, buf);
          v.encode_to(proto, buf);
        }
      }
      Value::Pairs(pairs) => {
        if proto == 3 {
          buf.push(b'*');
          buf.extend_from_slice(pairs.len().to_string().as_bytes());
          buf.extend_from_slice(b"\r\n");
          for (k, v) in pairs {
            buf.extend_from_slice(b"*2\r\n");
            k.encode_to(proto, buf);
            v.encode_to(proto, buf);
          }
        } else {
          buf.push(b'*');
          buf.extend_from_slice((pairs.len() * 2).to_string().as_bytes());
          buf.extend_from_slice(b"\r\n");
          for (k, v) in pairs {
            k.encode_to(proto, buf);
            v.encode_to(proto, buf);
          }
        }
      }
      Value::Boolean(val) => {
        if proto == 3 {
          buf.extend_from_slice(if *val { b"#t\r\n" } else { b"#f\r\n" });
        } else {
          buf.extend_from_slice(if *val { b":1\r\n" } else { b":0\r\n" });
        }
      }
    }
  }
}

/// Outcome of attempting to parse one request frame.
///
/// Distinguishing `Incomplete` from `Invalid` is essential: a connection whose
/// buffer can never form a valid frame must be closed, otherwise it would wait
/// forever for bytes that will never arrive.
#[derive(Debug, Clone, PartialEq)]
pub enum ParseResult {
  /// A complete frame: the value plus the number of bytes it occupied.
  Complete(Value, usize),
  /// A blank inline line (e.g. keepalive CRLF); consume and ignore it.
  Skip(usize),
  /// A partial frame; read more bytes and retry.
  Incomplete,
  /// Unparseable data; the connection should be closed.
  Invalid,
}

/// Parser for RESP arrays and inline commands
pub struct Parser;

/// Maximum length of a newline-free inline buffer before it is rejected.
const INLINE_MAX_SIZE: usize = 64 * 1024;

enum Frame {
  Value(Value),
  Incomplete,
  Invalid,
}

impl Parser {
  /// Parse one request frame from `buffer`.
  ///
  /// Supports RESP typed frames (`+ - : $ *`) and Redis' inline command format
  /// (space-separated arguments terminated by CRLF/LF), which tools such as
  /// `redis-benchmark -t PING` and `telnet` use.
  pub fn parse(buffer: &[u8]) -> ParseResult {
    if buffer.is_empty() {
      return ParseResult::Incomplete;
    }

    match buffer[0] {
      b'+' | b'-' | b':' | b'$' | b'*' => {
        let mut pos = 0;
        match Self::parse_value(buffer, &mut pos) {
          Frame::Value(v) => ParseResult::Complete(v, pos),
          Frame::Incomplete => ParseResult::Incomplete,
          Frame::Invalid => ParseResult::Invalid,
        }
      }
      _ => Self::parse_inline(buffer),
    }
  }

  fn parse_inline(buffer: &[u8]) -> ParseResult {
    let newline = match buffer.iter().position(|&b| b == b'\n') {
      Some(i) => i,
      None => {
        return if buffer.len() > INLINE_MAX_SIZE {
          ParseResult::Invalid
        } else {
          ParseResult::Incomplete
        };
      }
    };

    let mut end = newline;
    if end > 0 && buffer[end - 1] == b'\r' {
      end -= 1;
    }
    let line = &buffer[..end];
    let consumed = newline + 1;

    if line.iter().all(|b| b.is_ascii_whitespace()) {
      return ParseResult::Skip(consumed);
    }

    match Self::split_inline_args(line) {
      Some(args) => {
        let items = args
          .into_iter()
          .map(|arg| Value::BulkString(Some(arg)))
          .collect();
        ParseResult::Complete(Value::Array(Some(items)), consumed)
      }
      None => ParseResult::Invalid,
    }
  }

  fn split_inline_args(line: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut args = Vec::new();
    let mut i = 0;
    let n = line.len();

    while i < n {
      while i < n && (line[i] == b' ' || line[i] == b'\t') {
        i += 1;
      }
      if i >= n {
        break;
      }

      let mut arg = Vec::new();
      let mut in_single = false;
      let mut in_double = false;

      while i < n {
        let c = line[i];
        if in_single {
          if c == b'\'' {
            in_single = false;
          } else {
            arg.push(c);
          }
          i += 1;
        } else if in_double {
          if c == b'"' {
            in_double = false;
            i += 1;
          } else if c == b'\\' && i + 1 < n {
            i += 1;
            arg.push(line[i]);
            i += 1;
          } else {
            arg.push(c);
            i += 1;
          }
        } else if c == b' ' || c == b'\t' {
          break;
        } else if c == b'\'' {
          in_single = true;
          i += 1;
        } else if c == b'"' {
          in_double = true;
          i += 1;
        } else if c == b'\\' && i + 1 < n {
          i += 1;
          arg.push(line[i]);
          i += 1;
        } else {
          arg.push(c);
          i += 1;
        }
      }

      if in_single || in_double {
        return None;
      }
      args.push(arg);
    }

    Some(args)
  }

  fn parse_value(buffer: &[u8], pos: &mut usize) -> Frame {
    if *pos >= buffer.len() {
      return Frame::Incomplete;
    }

    let type_byte = buffer[*pos];
    *pos += 1;

    match type_byte {
      b'+' => Self::parse_simple_string(buffer, pos),
      b'-' => Self::parse_error(buffer, pos),
      b':' => Self::parse_integer(buffer, pos),
      b'$' => Self::parse_bulk_string(buffer, pos),
      b'*' => Self::parse_array(buffer, pos),
      _ => Frame::Invalid,
    }
  }

  fn parse_simple_string(buffer: &[u8], pos: &mut usize) -> Frame {
    match Self::read_line(buffer, pos) {
      Some(line) => Frame::Value(Value::SimpleString(
        String::from_utf8_lossy(line).to_string(),
      )),
      None => Frame::Incomplete,
    }
  }

  fn parse_error(buffer: &[u8], pos: &mut usize) -> Frame {
    match Self::read_line(buffer, pos) {
      Some(line) => Frame::Value(Value::Error(String::from_utf8_lossy(line).to_string())),
      None => Frame::Incomplete,
    }
  }

  fn parse_integer(buffer: &[u8], pos: &mut usize) -> Frame {
    let line = match Self::read_line(buffer, pos) {
      Some(l) => l,
      None => return Frame::Incomplete,
    };
    match String::from_utf8_lossy(line).parse::<i64>() {
      Ok(num) => Frame::Value(Value::Integer(num)),
      Err(_) => Frame::Invalid,
    }
  }

  fn parse_bulk_string(buffer: &[u8], pos: &mut usize) -> Frame {
    let line = match Self::read_line(buffer, pos) {
      Some(l) => l,
      None => return Frame::Incomplete,
    };

    let len = match String::from_utf8_lossy(line).parse::<i64>() {
      Ok(len) => len,
      Err(_) => return Frame::Invalid,
    };

    if len == -1 {
      return Frame::Value(Value::BulkString(None));
    }
    if len < -1 {
      return Frame::Invalid;
    }

    let len = len as usize;
    if *pos + len + 2 > buffer.len() {
      return Frame::Incomplete;
    }

    let data = buffer[*pos..*pos + len].to_vec();
    *pos += len + 2;
    Frame::Value(Value::BulkString(Some(data)))
  }

  fn parse_array(buffer: &[u8], pos: &mut usize) -> Frame {
    let line = match Self::read_line(buffer, pos) {
      Some(l) => l,
      None => return Frame::Incomplete,
    };

    let count = match String::from_utf8_lossy(line).parse::<i64>() {
      Ok(count) => count,
      Err(_) => return Frame::Invalid,
    };

    if count == -1 {
      return Frame::Value(Value::Array(None));
    }
    if count < -1 {
      return Frame::Invalid;
    }

    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
      match Self::parse_value(buffer, pos) {
        Frame::Value(v) => items.push(v),
        Frame::Incomplete => return Frame::Incomplete,
        Frame::Invalid => return Frame::Invalid,
      }
    }

    Frame::Value(Value::Array(Some(items)))
  }

  fn read_line<'a>(buffer: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    let start = *pos;

    for i in start..buffer.len().saturating_sub(1) {
      if buffer[i] == b'\r' && buffer[i + 1] == b'\n' {
        *pos = i + 2;
        return Some(&buffer[start..i]);
      }
    }

    None
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_simple_string() {
    match Parser::parse(b"+OK\r\n") {
      ParseResult::Complete(value, consumed) => {
        assert_eq!(value, Value::SimpleString("OK".to_string()));
        assert_eq!(consumed, 5);
      }
      other => panic!("expected Complete, got {other:?}"),
    }
  }

  #[test]
  fn test_parse_bulk_string() {
    match Parser::parse(b"$5\r\nhello\r\n") {
      ParseResult::Complete(value, consumed) => {
        assert_eq!(value, Value::BulkString(Some(b"hello".to_vec())));
        assert_eq!(consumed, 11);
      }
      other => panic!("expected Complete, got {other:?}"),
    }
  }

  #[test]
  fn test_parse_array() {
    let data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    match Parser::parse(data) {
      ParseResult::Complete(value, consumed) => {
        match value {
          Value::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Value::BulkString(Some(b"SET".to_vec())));
            assert_eq!(arr[1], Value::BulkString(Some(b"key".to_vec())));
            assert_eq!(arr[2], Value::BulkString(Some(b"value".to_vec())));
          }
          _ => panic!("expected array"),
        }
        assert_eq!(consumed, data.len());
      }
      other => panic!("expected Complete, got {other:?}"),
    }
  }

  #[test]
  fn test_parse_incomplete_array_returns_incomplete() {
    assert_eq!(
      Parser::parse(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n"),
      ParseResult::Incomplete
    );
  }

  #[test]
  fn test_parse_malformed_bulk_length_is_invalid() {
    assert_eq!(Parser::parse(b"$abc\r\n"), ParseResult::Invalid);
  }

  #[test]
  fn test_parse_malformed_integer_is_invalid() {
    assert_eq!(Parser::parse(b":notanumber\r\n"), ParseResult::Invalid);
  }

  #[test]
  fn test_parse_inline_ping() {
    match Parser::parse(b"PING\r\n") {
      ParseResult::Complete(Value::Array(Some(items)), consumed) => {
        assert_eq!(items, vec![Value::BulkString(Some(b"PING".to_vec()))]);
        assert_eq!(consumed, 6);
      }
      other => panic!("expected inline PING array, got {other:?}"),
    }
  }

  #[test]
  fn test_parse_inline_command_with_args() {
    let data = b"SET mykey myvalue\n";
    match Parser::parse(data) {
      ParseResult::Complete(Value::Array(Some(items)), consumed) => {
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], Value::BulkString(Some(b"SET".to_vec())));
        assert_eq!(items[1], Value::BulkString(Some(b"mykey".to_vec())));
        assert_eq!(items[2], Value::BulkString(Some(b"myvalue".to_vec())));
        assert_eq!(consumed, data.len());
      }
      other => panic!("expected inline array, got {other:?}"),
    }
  }

  #[test]
  fn test_parse_inline_quoted_argument() {
    match Parser::parse(b"SET mykey \"hello world\"\r\n") {
      ParseResult::Complete(Value::Array(Some(items)), _) => {
        assert_eq!(items.len(), 3);
        assert_eq!(items[2], Value::BulkString(Some(b"hello world".to_vec())));
      }
      other => panic!("expected inline array, got {other:?}"),
    }
  }

  #[test]
  fn test_parse_inline_incomplete_returns_incomplete() {
    assert_eq!(Parser::parse(b"PING"), ParseResult::Incomplete);
  }

  #[test]
  fn test_parse_inline_blank_line_is_skipped() {
    assert_eq!(Parser::parse(b"\r\n"), ParseResult::Skip(2));
    assert_eq!(Parser::parse(b"   \n"), ParseResult::Skip(4));
  }

  #[test]
  fn test_parse_inline_unbalanced_quotes_is_invalid() {
    assert_eq!(
      Parser::parse(b"SET key \"unterminated\r\n"),
      ParseResult::Invalid
    );
  }

  #[test]
  fn test_parse_non_resp_bytes_are_treated_as_inline_command() {
    match Parser::parse(b"\x00\x01\r\n") {
      ParseResult::Complete(Value::Array(Some(items)), consumed) => {
        assert_eq!(items, vec![Value::BulkString(Some(vec![0x00, 0x01]))]);
        assert_eq!(consumed, 4);
      }
      other => panic!("expected inline command, got {other:?}"),
    }
  }

  #[test]
  fn test_parse_oversized_inline_without_newline_is_invalid() {
    let data = vec![b'x'; INLINE_MAX_SIZE + 1];
    assert_eq!(Parser::parse(&data), ParseResult::Invalid);
  }

  #[test]
  fn test_encode_simple_string() {
    assert_eq!(Value::SimpleString("OK".to_string()).encode(), b"+OK\r\n");
  }

  #[test]
  fn test_encode_bulk_string() {
    assert_eq!(
      Value::BulkString(Some(b"hello".to_vec())).encode(),
      b"$5\r\nhello\r\n"
    );
  }
}
