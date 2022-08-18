//! Arrow message stream types.

use std::{
    cmp,
    fmt::{self, Display, Formatter},
    io,
    mem::MaybeUninit,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BytesMut};
use futures::{ready, Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{
    msg::{control::InvalidControlMessage, ArrowMessage, InvalidMessage},
    utils::{Decode, Encode},
};

/// Default capacity of the input buffer.
const INPUT_BUFFER_CAPACITY: usize = 0x100_0000;

/// Capacity of the output buffer.
///
/// This is just a soft limit that will require calling `poll_ready` on the
/// corresponding sink.
const OUTPUT_BUFFER_CAPACITY: usize = 0x2_0000;

/// Message stream error.
#[derive(Debug)]
pub enum Error {
    UnsupportedProtocolVersion(u8),
    MessageSizeExceeded,
    InvalidControlMessage(InvalidControlMessage),
    IO(io::Error),
}

impl Error {
    /// Create a new error indicating an unexpected EOF.
    fn unexpected_eof() -> Self {
        Self::IO(io::Error::from(io::ErrorKind::UnexpectedEof))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedProtocolVersion(v) => {
                write!(f, "unsupported version of the Arrow Protocol: {}", v)
            }
            Self::MessageSizeExceeded => f.write_str("maximum Arrow Message size exceeded"),
            Self::InvalidControlMessage(err) => {
                write!(f, "invalid Arrow Control Protocol message: {}", err)
            }
            Self::IO(err) => write!(f, "IO error: {}", err),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::IO(err)
    }
}

/// Arrow message stream.
pub struct ArrowMessageStream<IO> {
    inner_io: IO,
    input_capacity: usize,
    input_buffer: BytesMut,
    output_buffer: BytesMut,
}

impl<IO> ArrowMessageStream<IO> {
    /// Create a new message stream from a given IO.
    pub fn new(io: IO) -> Self {
        Self {
            inner_io: io,
            input_capacity: INPUT_BUFFER_CAPACITY,
            input_buffer: BytesMut::new(),
            output_buffer: BytesMut::new(),
        }
    }

    /// Set limit for the incoming message size.
    ///
    /// Output messages are not affected by this limit.
    pub fn with_max_message_size(mut self, limit: usize) -> Self {
        self.input_capacity = limit;
        self
    }
}

impl<IO> Stream for ArrowMessageStream<IO>
where
    IO: AsyncRead + Unpin,
{
    type Item = Result<ArrowMessage, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut buf: [MaybeUninit<u8>; 4096] = unsafe { MaybeUninit::uninit().assume_init() };

        loop {
            match ArrowMessage::decode(&mut self.input_buffer) {
                Ok(msg) => return Poll::Ready(Some(Ok(msg))),
                Err(InvalidMessage::IncompleteMessage) => {
                    let available = self.input_buffer.len();

                    if available >= self.input_capacity {
                        return Poll::Ready(Some(Err(Error::MessageSizeExceeded)));
                    }

                    let read = cmp::min(self.input_capacity - available, buf.len());

                    let mut buf = ReadBuf::uninit(&mut buf[..read]);

                    ready!(AsyncRead::poll_read(
                        Pin::new(&mut self.inner_io),
                        cx,
                        &mut buf
                    ))?;

                    let filled = buf.filled();

                    if filled.is_empty() {
                        if self.input_buffer.is_empty() {
                            return Poll::Ready(None);
                        } else {
                            return Poll::Ready(Some(Err(Error::unexpected_eof())));
                        }
                    } else {
                        self.input_buffer.extend_from_slice(filled);
                    }
                }
                Err(InvalidMessage::UnsupportedProtocolVersion(v)) => {
                    return Poll::Ready(Some(Err(Error::UnsupportedProtocolVersion(v))))
                }
                Err(InvalidMessage::InvalidControlMessage(err)) => {
                    return Poll::Ready(Some(Err(Error::InvalidControlMessage(err))))
                }
            }
        }
    }
}

impl<IO> ArrowMessageStream<IO>
where
    IO: AsyncWrite + Unpin,
{
    fn poll_write(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let len = ready!(AsyncWrite::poll_write(
            Pin::new(&mut self.inner_io),
            cx,
            &self.output_buffer
        ))?;

        if len == 0 && !self.output_buffer.is_empty() {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
        }

        self.output_buffer.advance(len);

        Poll::Ready(Ok(()))
    }
}

impl<IO> Sink<ArrowMessage> for ArrowMessageStream<IO>
where
    IO: AsyncWrite + Unpin,
{
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        while self.output_buffer.len() > OUTPUT_BUFFER_CAPACITY {
            ready!(self.poll_write(cx))?;
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, msg: ArrowMessage) -> Result<(), Self::Error> {
        self.output_buffer.reserve(msg.size());

        msg.encode(&mut self.output_buffer);

        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        while !self.output_buffer.is_empty() {
            ready!(self.poll_write(cx))?;
        }

        AsyncWrite::poll_flush(Pin::new(&mut self.inner_io), cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        while !self.output_buffer.is_empty() {
            ready!(self.poll_write(cx))?;
        }

        AsyncWrite::poll_shutdown(Pin::new(&mut self.inner_io), cx)
    }
}
