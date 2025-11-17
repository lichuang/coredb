use std::time::Duration;

use bytes::Bytes;
use tracing::instrument;

use crate::errors::Error;
use crate::errors::Result;
use crate::protocol::Frame;
use crate::protocol::ParseError::EndOfStream;
use crate::protocol::Parser;
use crate::server::Connection;
use crate::server::KVApi;

#[derive(Debug)]
pub struct Set {
  key: String,

  value: Bytes,

  expire: Option<Duration>,
}

impl Set {
  pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
    Set {
      key: key.to_string(),
      value,
      expire,
    }
  }

  pub fn key(&self) -> &str {
    &self.key
  }

  pub fn value(&self) -> &Bytes {
    &self.value
  }

  pub fn expire(&self) -> Option<Duration> {
    self.expire
  }

  pub(crate) fn parse_frames(parse: &mut Parser) -> Result<Set> {
    let key = parse.next_string()?;

    let value = parse.next_bytes()?;

    let mut expire = None;

    match parse.next_string() {
      Ok(s) if s.to_uppercase() == "EX" => {
        let secs = parse.next_int()?;
        expire = Some(Duration::from_secs(secs));
      }
      Ok(s) if s.to_uppercase() == "PX" => {
        let ms = parse.next_int()?;
        expire = Some(Duration::from_millis(ms));
      }
      Ok(_) => {
        return Err(Error::RedisProtocol(
          "currently `SET` only supports the expiration option".to_string(),
        ));
      }
      Err(EndOfStream) => {}
      Err(err) => return Err(err.into()),
    }

    Ok(Set { key, value, expire })
  }

  pub(crate) async fn apply(self, kv_api: impl KVApi, dst: &mut Connection) -> Result<()> {
    // Set the value in the shared database state.
    // db.set(self.key, self.value, self.expire);

    // Create a success response and write it to `dst`.
    // let response = Frame::Simple("OK".to_string());
    // debug!(?response);
    // dst.write_frame(&response).await?;

    println!(
      "set {} to {}",
      self.key,
      String::from_utf8(self.value.to_vec()).unwrap()
    );
    Ok(())
  }

  /// Converts the command into an equivalent `Frame`.
  ///
  /// This is called by the client when encoding a `Set` command to send to
  /// the server.
  pub(crate) fn into_frame(self) -> Frame {
    let mut frame = Frame::array();
    frame.push_bulk(Bytes::from("set".as_bytes()));
    frame.push_bulk(Bytes::from(self.key.into_bytes()));
    frame.push_bulk(self.value);
    if let Some(ms) = self.expire {
      frame.push_bulk(Bytes::from("px".as_bytes()));
      frame.push_int(ms.as_millis() as u64);
    }
    frame
  }
}
