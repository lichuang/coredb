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

/// Parser for RESP protocol
pub struct Parser;

impl Parser {
  /// Parse RESP data from buffer, return (Value, consumed_bytes) if successful
  pub fn parse(buffer: &[u8]) -> Option<(Value, usize)> {
    if buffer.is_empty() {
      return None;
    }

    let mut pos = 0;
    let result = Self::parse_value(buffer, &mut pos)?;
    Some((result, pos))
  }

  fn parse_value(buffer: &[u8], pos: &mut usize) -> Option<Value> {
    if *pos >= buffer.len() {
      return None;
    }

    let type_byte = buffer[*pos];
    *pos += 1;

    match type_byte {
      b'+' => Self::parse_simple_string(buffer, pos),
      b'-' => Self::parse_error(buffer, pos),
      b':' => Self::parse_integer(buffer, pos),
      b'$' => Self::parse_bulk_string(buffer, pos),
      b'*' => Self::parse_array(buffer, pos),
      _ => None,
    }
  }

  fn parse_simple_string(buffer: &[u8], pos: &mut usize) -> Option<Value> {
    let line = Self::read_line(buffer, pos)?;
    Some(Value::SimpleString(
      String::from_utf8_lossy(line).to_string(),
    ))
  }

  fn parse_error(buffer: &[u8], pos: &mut usize) -> Option<Value> {
    let line = Self::read_line(buffer, pos)?;
    Some(Value::Error(String::from_utf8_lossy(line).to_string()))
  }

  fn parse_integer(buffer: &[u8], pos: &mut usize) -> Option<Value> {
    let line = Self::read_line(buffer, pos)?;
    let num = String::from_utf8_lossy(line).parse::<i64>().ok()?;
    Some(Value::Integer(num))
  }

  fn parse_bulk_string(buffer: &[u8], pos: &mut usize) -> Option<Value> {
    let line = Self::read_line(buffer, pos)?;
    let len = String::from_utf8_lossy(line).parse::<i64>().ok()?;

    if len == -1 {
      return Some(Value::BulkString(None));
    }

    if len < 0 {
      return None;
    }

    let len = len as usize;

    // Check if we have enough data (len + \r\n)
    if *pos + len + 2 > buffer.len() {
      return None;
    }

    let data = buffer[*pos..*pos + len].to_vec();
    *pos += len + 2; // +2 for \r\n

    Some(Value::BulkString(Some(data)))
  }

  fn parse_array(buffer: &[u8], pos: &mut usize) -> Option<Value> {
    let line = Self::read_line(buffer, pos)?;
    let count = String::from_utf8_lossy(line).parse::<i64>().ok()?;

    if count == -1 {
      return Some(Value::Array(None));
    }

    if count < 0 {
      return None;
    }

    let count = count as usize;
    let mut items = Vec::with_capacity(count);

    for _ in 0..count {
      items.push(Self::parse_value(buffer, pos)?);
    }

    Some(Value::Array(Some(items)))
  }

  fn read_line<'a>(buffer: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    let start = *pos;

    // Find \r\n
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
    let data = b"+OK\r\n";
    let (value, consumed) = Parser::parse(data).unwrap();
    assert_eq!(value, Value::SimpleString("OK".to_string()));
    assert_eq!(consumed, 5);
  }

  #[test]
  fn test_parse_bulk_string() {
    let data = b"$5\r\nhello\r\n";
    let (value, consumed) = Parser::parse(data).unwrap();
    assert_eq!(value, Value::BulkString(Some(b"hello".to_vec())));
    assert_eq!(consumed, 11);
  }

  #[test]
  fn test_parse_array() {
    // *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
    let data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let (value, consumed) = Parser::parse(data).unwrap();

    match value {
      Value::Array(Some(arr)) => {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Value::BulkString(Some(b"SET".to_vec())));
        assert_eq!(arr[1], Value::BulkString(Some(b"key".to_vec())));
        assert_eq!(arr[2], Value::BulkString(Some(b"value".to_vec())));
      }
      _ => panic!("Expected array"),
    }
    assert_eq!(consumed, data.len());
  }

  #[test]
  fn test_encode_simple_string() {
    let value = Value::SimpleString("OK".to_string());
    assert_eq!(value.encode(), b"+OK\r\n");
  }

  #[test]
  fn test_encode_bulk_string() {
    let value = Value::BulkString(Some(b"hello".to_vec()));
    assert_eq!(value.encode(), b"$5\r\nhello\r\n");
  }
}
