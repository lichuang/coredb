use std::io::Cursor;

use bytes::Buf;
use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::info;

use super::shutdown::Shutdown;
use crate::errors::Result;
use crate::protocol::Command;
use crate::protocol::Frame;
use crate::protocol::ParseError;

struct RedisStream {
  stream: BufWriter<TcpStream>,

  buffer: BytesMut,
}

pub struct Connection {
  stream: RedisStream,

  shutdown: Shutdown,

  _shutdown_complete: mpsc::Sender<()>,
}

impl Connection {
  pub fn new(
    socket: TcpStream,
    shutdown: Shutdown,
    shutdown_complete: mpsc::Sender<()>,
  ) -> Connection {
    Connection {
      stream: RedisStream::new(socket),
      shutdown,
      _shutdown_complete: shutdown_complete,
    }
  }

  pub async fn run(&mut self) -> Result<()> {
    info!("start process client request");
    while !self.shutdown.is_shutdown() {
      let maybe_frame = tokio::select! {
          res = self.stream. read_frame() => res?,
          _ = self.shutdown.recv() => {
              // If a shutdown signal is received, return from `run`.
              // This will result in the task terminating.
              return Ok(());
          }
      };

      let frame = match maybe_frame {
        Some(frame) => frame,
        None => return Ok(()),
      };

      let cmd = Command::from_frame(frame)?;

      debug!(?cmd);

      // cmd.apply(&mut self, &mut self.shutdown).await?;
    }
    Ok(())
  }

  pub async fn write_frame(&mut self, frame: &Frame) -> std::io::Result<()> {
    self.stream.write_frame(frame).await
  }
}

impl RedisStream {
  pub fn new(socket: TcpStream) -> RedisStream {
    Self {
      stream: BufWriter::new(socket),
      buffer: BytesMut::with_capacity(4 * 1024),
    }
  }

  async fn read_frame(&mut self) -> Result<Option<Frame>> {
    loop {
      if let Some(frame) = self.parse_frame()? {
        return Ok(Some(frame));
      }

      if self.stream.read_buf(&mut self.buffer).await? == 0 {
        if self.buffer.is_empty() {
          return Ok(None);
        } else {
          return Err(crate::errors::Error::Connection(
            "connection reset by peer".into(),
          ));
        }
      }
    }
  }

  fn parse_frame(&mut self) -> Result<Option<Frame>, ParseError> {
    use crate::protocol::Error::Incomplete;

    let mut buf = Cursor::new(&self.buffer[..]);

    match Frame::check(&mut buf) {
      Ok(_) => {
        let len = buf.position() as usize;

        buf.set_position(0);

        let frame = Frame::parse(&mut buf)?;

        self.buffer.advance(len);

        Ok(Some(frame))
      }
      Err(Incomplete) => Ok(None),
      Err(e) => Err(e.into()),
    }
  }

  pub async fn write_frame(&mut self, frame: &Frame) -> std::io::Result<()> {
    match frame {
      Frame::Array(val) => {
        self.stream.write_u8(b'*').await?;

        self.write_decimal(val.len() as u64).await?;

        for entry in &**val {
          self.write_value(entry).await?;
        }
      }
      _ => self.write_value(frame).await?,
    }

    self.stream.flush().await
  }

  async fn write_value(&mut self, frame: &Frame) -> std::io::Result<()> {
    match frame {
      Frame::Simple(val) => {
        self.stream.write_u8(b'+').await?;
        self.stream.write_all(val.as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;
      }
      Frame::Error(val) => {
        self.stream.write_u8(b'-').await?;
        self.stream.write_all(val.as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;
      }
      Frame::Integer(val) => {
        self.stream.write_u8(b':').await?;
        self.write_decimal(*val).await?;
      }
      Frame::Null => {
        self.stream.write_all(b"$-1\r\n").await?;
      }
      Frame::Bulk(val) => {
        let len = val.len();

        self.stream.write_u8(b'$').await?;
        self.write_decimal(len as u64).await?;
        self.stream.write_all(val).await?;
        self.stream.write_all(b"\r\n").await?;
      }
      Frame::Array(_val) => unreachable!(),
    }

    Ok(())
  }

  async fn write_decimal(&mut self, val: u64) -> std::io::Result<()> {
    use std::io::Write;

    let mut buf = [0u8; 20];
    let mut buf = Cursor::new(&mut buf[..]);
    write!(&mut buf, "{}", val)?;

    let pos = buf.position() as usize;
    self.stream.write_all(&buf.get_ref()[..pos]).await?;
    self.stream.write_all(b"\r\n").await?;

    Ok(())
  }
}
