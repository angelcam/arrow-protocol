use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::{Buf, BytesMut};
use futures::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder, Framed};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, SizeError, Unaligned,
    byteorder::network_endian::U32,
};

use crate::v3::{
    PROTOCOL_VERSION,
    connection::io::{BaseIO, TXState},
    msg::EncodedMessage,
};

/// Base connection error.
#[derive(Debug)]
pub enum BaseConnectionError {
    UnsupportedProtocolVersion(u8),
    UnknownMessageType(u8),
    PayloadSizeExceeded,
    UnexpectedEOF,
    IO(io::Error),
}

impl From<io::Error> for BaseConnectionError {
    fn from(err: io::Error) -> Self {
        Self::IO(err)
    }
}

/// Base connection builder.
pub struct BaseConnectionBuilder {
    max_rx_payload_size: u32,
    write_timeout: Duration,
    close_timeout: Duration,
}

impl BaseConnectionBuilder {
    /// Create a new base connection builder.
    pub const fn new() -> Self {
        Self {
            max_rx_payload_size: 65_536,
            write_timeout: Duration::from_secs(60),
            close_timeout: Duration::from_secs(60),
        }
    }

    /// Set the maximum RX payload size.
    ///
    /// All decoded messages will be limited to the specified maximum payload
    /// size. If an incoming message exceeds this size, an error will be
    /// returned.
    pub const fn with_max_rx_payload_size(mut self, size: u32) -> Self {
        self.max_rx_payload_size = size;
        self
    }

    /// Set the write timeout.
    pub const fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Set the close timeout.
    pub const fn with_close_timeout(mut self, timeout: Duration) -> Self {
        self.close_timeout = timeout;
        self
    }

    /// Build the base connection.
    pub fn build<T>(self, io: T) -> BaseConnection<T> {
        let codec = MessageCodec::new(self.max_rx_payload_size);
        let io = BaseIO::new(io, self.write_timeout, self.close_timeout);
        let inner = Framed::new(io, codec);

        BaseConnection { inner }
    }
}

pin_project_lite::pin_project! {
    /// Base connection.
    ///
    /// The connection handles only low-level message framing.
    pub struct BaseConnection<T> {
        #[pin]
        inner: Framed<BaseIO<T>, MessageCodec>,
    }
}

impl<T> Stream for BaseConnection<T>
where
    T: AsyncRead,
{
    type Item = Result<EncodedMessage, BaseConnectionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        let inner = this.inner.as_mut();

        let res = match futures::ready!(inner.poll_next(cx)) {
            Some(Err(err)) if matches!(err, BaseConnectionError::UnexpectedEOF) => {
                let io = this.inner.get_ref();

                let s = io.tx_state();

                if matches!(s, TXState::Open | TXState::Broken) {
                    Some(Err(err))
                } else {
                    None
                }
            }
            r => r,
        };

        Poll::Ready(res)
    }
}

impl<T> Sink<EncodedMessage> for BaseConnection<T>
where
    T: AsyncRead + AsyncWrite,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();

        this.inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, msg: EncodedMessage) -> Result<(), Self::Error> {
        let this = self.project();

        this.inner.start_send(msg)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();

        this.inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();

        this.inner.poll_close(cx)
    }
}

/// Message codec.
struct MessageCodec {
    max_rx_payload_size: u32,
}

impl MessageCodec {
    /// Create a new message codec.
    ///
    /// All decoded messages will be limited to the specified maximum payload
    /// size. If an incoming message exceeds this size, an error will be
    /// returned.
    const fn new(max_rx_payload_size: u32) -> Self {
        Self {
            max_rx_payload_size,
        }
    }
}

impl Decoder for MessageCodec {
    type Item = EncodedMessage;
    type Error = BaseConnectionError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let header_size = std::mem::size_of::<MessageHeader>();

        let Ok((header, _)) = MessageHeader::ref_from_prefix(buf).map_err(SizeError::from) else {
            return Ok(None);
        };

        let version = header.version;

        if version != PROTOCOL_VERSION {
            return Err(BaseConnectionError::UnsupportedProtocolVersion(version));
        }

        let kind = header
            .kind
            .try_into()
            .map_err(|_| BaseConnectionError::UnknownMessageType(header.kind))?;

        let payload_size = header.size.get();

        if payload_size > self.max_rx_payload_size {
            return Err(BaseConnectionError::PayloadSizeExceeded);
        } else if buf.len() < (header_size + (payload_size as usize)) {
            return Ok(None);
        }

        buf.advance(header_size);

        let payload = buf.split_to(payload_size as usize);

        let res = EncodedMessage::new_with_version(version, kind, payload.freeze());

        Ok(Some(res))
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(msg) = self.decode(src)? {
            Ok(Some(msg))
        } else if src.is_empty() {
            Ok(None)
        } else {
            Err(BaseConnectionError::UnexpectedEOF)
        }
    }
}

impl Encoder<EncodedMessage> for MessageCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: EncodedMessage, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let kind = msg.kind();
        let payload = msg.data();

        let payload_size = payload.len();

        let header = MessageHeader {
            version: PROTOCOL_VERSION,
            kind: kind as u8,
            size: U32::new(payload_size as u32),
        };

        let header = header.as_bytes();

        let header_len = header.len();

        buf.reserve(header_len + payload_size);
        buf.extend_from_slice(header);
        buf.extend_from_slice(payload);

        Ok(())
    }
}

/// Message header.
#[derive(Copy, Clone, KnownLayout, Immutable, Unaligned, IntoBytes, FromBytes)]
#[repr(C)]
struct MessageHeader {
    version: u8,
    kind: u8,
    size: U32,
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    use bytes::{Bytes, BytesMut};
    use futures::{SinkExt, StreamExt};
    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

    use super::{BaseConnectionBuilder, BaseConnectionError};

    use crate::v3::msg::{EncodedMessage, MessageKind};

    struct FakeIO {
        rx: Bytes,
        tx: BytesMut,
    }

    impl FakeIO {
        /// Create a new fake IO.
        fn new<T>(rx: T) -> Self
        where
            T: Into<Bytes>,
        {
            Self {
                rx: rx.into(),
                tx: BytesMut::new(),
            }
        }
    }

    impl AsyncRead for FakeIO {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let take = self.rx.len().min(buf.remaining());

            let chunk = self.rx.split_to(take);

            buf.put_slice(&chunk);

            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for FakeIO {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.tx.extend_from_slice(buf);

            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    fn create_message(kind: MessageKind, payload: &[u8]) -> EncodedMessage {
        EncodedMessage::new(kind, Bytes::copy_from_slice(payload))
    }

    #[tokio::test]
    async fn test_unsupported_protocol_version() {
        let fake_io = FakeIO::new(&[0xff, 0x02, 0x00, 0x00, 0x00, 0x01, 0x01][..]);

        let connection = BaseConnectionBuilder::new()
            .with_max_rx_payload_size(1024)
            .build(fake_io);

        futures::pin_mut!(connection);

        let err = connection.next().await.unwrap().err().unwrap();

        assert!(matches!(
            err,
            BaseConnectionError::UnsupportedProtocolVersion(0xff)
        ));
    }

    #[tokio::test]
    async fn test_unknown_message_type() {
        let fake_io = FakeIO::new(&[0x03, 0xff, 0x00, 0x00, 0x00, 0x01, 0x01][..]);

        let connection = BaseConnectionBuilder::new()
            .with_max_rx_payload_size(1024)
            .build(fake_io);

        futures::pin_mut!(connection);

        let err = connection.next().await.unwrap().err().unwrap();

        assert!(matches!(err, BaseConnectionError::UnknownMessageType(0xff)));
    }

    #[tokio::test]
    async fn test_payload_size_exceeded() {
        let fake_io = FakeIO::new(&[0x03, 0x06, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00][..]);

        let connection = BaseConnectionBuilder::new()
            .with_max_rx_payload_size(1)
            .build(fake_io);

        futures::pin_mut!(connection);

        let err = connection.next().await.unwrap().err().unwrap();

        assert!(matches!(err, BaseConnectionError::PayloadSizeExceeded));
    }

    #[tokio::test]
    async fn test_unexpected_eof() {
        let fake_io = FakeIO::new(&[0x03, 0x06, 0x00, 0x00, 0x00, 0x02, 0x00][..]);

        let connection = BaseConnectionBuilder::new()
            .with_max_rx_payload_size(1024)
            .build(fake_io);

        futures::pin_mut!(connection);

        let err = connection.next().await.unwrap().err().unwrap();

        assert!(matches!(err, BaseConnectionError::UnexpectedEOF));
    }

    #[tokio::test]
    async fn test_framing() {
        let mut data = BytesMut::new();

        data.extend_from_slice(&[0x03, 0x02, 0x00, 0x00, 0x00, 0x01, 0x01]);
        data.extend_from_slice(&[0x03, 0x06, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00]);
        data.extend_from_slice(&[0x03, 0x07, 0x00, 0x00, 0x00, 0x02, 0xdd, 0xee]);

        let data = data.freeze();

        let mut fake_io = FakeIO::new(data.clone());

        let connection = BaseConnectionBuilder::new()
            .with_max_rx_payload_size(1024)
            .build(&mut fake_io);

        futures::pin_mut!(connection);

        let (mut tx, rx) = connection.split();

        let messages = rx
            .filter_map(|res| futures::future::ready(res.ok()))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(messages.len(), 3);

        let msg = &messages[0];

        assert_eq!(msg.kind(), MessageKind::Error);
        assert_eq!(msg.data(), &[0x01][..]);

        let msg = &messages[1];

        assert_eq!(msg.kind(), MessageKind::Ping);
        assert_eq!(msg.data(), &[0x00, 0x00][..]);

        let msg = &messages[2];

        assert_eq!(msg.kind(), MessageKind::Pong);
        assert_eq!(msg.data(), &[0xdd, 0xee][..]);

        tx.send(create_message(MessageKind::Error, &[0x01]))
            .await
            .unwrap();

        tx.send(create_message(MessageKind::Ping, &[0x00, 0x00]))
            .await
            .unwrap();

        tx.send(create_message(MessageKind::Pong, &[0xdd, 0xee]))
            .await
            .unwrap();

        std::mem::drop(tx);

        assert_eq!(fake_io.tx, data);
    }

    #[tokio::test(start_paused = true)]
    async fn test_partial_frame_after_local_close_is_eof() {
        let (local, mut remote) = tokio::io::duplex(1024);

        let mut data = BytesMut::new();

        // one complete PING message followed by an incomplete one
        data.extend_from_slice(&[0x03, 0x06, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00]);
        data.extend_from_slice(&[0x03, 0x06, 0x00, 0x00, 0x00, 0x02, 0x00]);

        remote.write_all(&data).await.unwrap();

        let connection = BaseConnectionBuilder::new()
            .with_max_rx_payload_size(1024)
            .build(local);

        let (mut tx, mut rx) = connection.split();

        let msg = rx.next().await.unwrap().unwrap();

        assert_eq!(msg.kind(), MessageKind::Ping);

        let close = tokio::spawn(async move { tx.close().await });

        // the incomplete message is not an error after a local close
        assert!(rx.next().await.is_none());

        // the lingering close completes after the remote peer closes the
        // connection
        std::mem::drop(remote);

        close.await.unwrap().unwrap();
    }
}
