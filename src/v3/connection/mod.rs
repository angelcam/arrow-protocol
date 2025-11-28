mod base;
mod ping;

use std::{
    fmt::{self, Display, Formatter},
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    FutureExt, Sink, SinkExt, Stream, StreamExt,
    channel::mpsc::{self, SendError},
    future::{AbortHandle, Abortable, Fuse},
    ready,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::JoinHandle,
};

use self::{
    base::{BaseConnection, BaseConnectionError},
    ping::{IncomingPongSender, OutgoingPingReceiver, PingPongError, PingPongErrorReceiver},
};

use crate::v3::{
    error::Error,
    msg::{
        DecodeMessage, EncodeMessage, EncodedMessage, Message, MessageEncoder, MessageKind,
        error::ErrorMessage,
        ping::{PingMessage, PongMessage},
    },
};

pub use self::ping::PingPongHandler;

/// Connection builder.
pub struct ConnectionBuilder {
    max_rx_payload_size: u32,
}

impl ConnectionBuilder {
    /// Create a new connection builder.
    const fn new() -> Self {
        Self {
            max_rx_payload_size: 65536,
        }
    }

    /// Set the maximum payload size for received messages.
    pub const fn with_max_rx_payload_size(mut self, size: u32) -> Self {
        self.max_rx_payload_size = size;
        self
    }

    /// Build the connection.
    ///
    /// The method returns the connection and a PING-PONG handler. The
    /// PING-PONG handler should be spawned as a separate task after the
    /// initial handshake is complete. The task will be terminated when the
    /// connection is dropped.
    pub fn build<T>(self, io: T) -> (Connection, PingPongHandler)
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let connection = BaseConnection::new(io, self.max_rx_payload_size);

        let (tx, rx) = connection.split();

        let (incoming_message_tx, incoming_message_rx) = mpsc::channel(4);
        let (outgoing_message_tx, outgoing_message_rx) = mpsc::channel(4);
        let (outgoing_ping_tx, outgoing_ping_rx) = mpsc::channel(4);
        let (incoming_pong_tx, incoming_pong_rx) = mpsc::channel(4);

        let (outgoing_ping_rx, outgoing_ping_abort_handle) =
            futures::stream::abortable(outgoing_ping_rx);

        let (ping_pong_handler, timeout_rx, ping_pong_abort_handle) =
            PingPongHandler::new(outgoing_ping_tx, incoming_pong_rx);

        let reader = IncomingMessageReader {
            outgoing_message_tx: outgoing_message_tx.clone(),
            incoming_message_tx,
            incoming_pong_tx,
            timeout_rx: timeout_rx.fuse(),
            encoder: MessageEncoder::new(),
        };

        // TERMINATION: The task will be terminated when the base connection
        //   returns `None` or an error OR when the connection is dropped.
        let reader_task = tokio::spawn(async move {
            let _ = reader.read_all(rx).await;
        });

        let sender = OutgoingMessageSender {
            outgoing_message_rx,
            outgoing_ping_rx,
        };

        // TERMINATION: The task will be terminated when the reader task is
        //   terminated AND the connection is closed/dropped and all messages
        //   have been sent.
        tokio::spawn(sender.send_all(tx));

        let res = Connection {
            rx: incoming_message_rx,
            tx: outgoing_message_tx,

            reader_task,

            outgoing_ping_abort_handle,
            ping_pong_abort_handle,
        };

        (res, ping_pong_handler)
    }
}

/// Connection.
pub struct Connection {
    rx: IncomingMessageRx,
    tx: OutgoingMessageTx,

    reader_task: JoinHandle<()>,

    outgoing_ping_abort_handle: AbortHandle,
    ping_pong_abort_handle: AbortHandle,
}

impl Connection {
    /// Get a connection builder.
    pub const fn builder() -> ConnectionBuilder {
        ConnectionBuilder::new()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.reader_task.abort();

        self.outgoing_ping_abort_handle.abort();
        self.ping_pong_abort_handle.abort();
    }
}

impl Stream for Connection {
    type Item = Result<EncodedMessage, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_next_unpin(cx)
    }
}

impl Sink<EncodedMessage> for Connection {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.tx
            .poll_ready_unpin(cx)
            .map_err(|_| Error::from_static_msg("connection closed"))
    }

    fn start_send(mut self: Pin<&mut Self>, msg: EncodedMessage) -> Result<(), Self::Error> {
        self.tx
            .start_send_unpin(msg)
            .map_err(|_| Error::from_static_msg("connection closed"))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.tx
            .poll_flush_unpin(cx)
            .map_err(|_| Error::from_static_msg("connection closed"))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.tx
            .poll_close_unpin(cx)
            .map_err(|_| Error::from_static_msg("connection closed"))
    }
}

/// Incoming message sender.
type IncomingMessageTx = mpsc::Sender<Result<EncodedMessage, Error>>;

/// Incoming message receiver.
type IncomingMessageRx = mpsc::Receiver<Result<EncodedMessage, Error>>;

/// Incoming message reader.
struct IncomingMessageReader {
    outgoing_message_tx: OutgoingMessageTx,
    incoming_message_tx: IncomingMessageTx,
    incoming_pong_tx: IncomingPongSender,
    timeout_rx: Fuse<PingPongErrorReceiver>,
    encoder: MessageEncoder,
}

impl IncomingMessageReader {
    /// Read all incoming messages from a given stream.
    async fn read_all<T, E>(mut self, rx: T) -> Result<(), SendError>
    where
        T: Stream<Item = Result<EncodedMessage, E>>,
        E: Into<ConnectionError>,
    {
        let rx = rx.fuse();

        futures::pin_mut!(rx);

        let err = loop {
            let mut timeout_rx = &mut self.timeout_rx;

            let msg = futures::select! {
                next = rx.next() => match next {
                    Some(Ok(msg)) => self.process_message(msg).await,
                    Some(Err(err)) => Err(err.into()),
                    None => return Ok(()),
                },
                timeout = timeout_rx => match timeout {
                    Ok(err) => Err(err.into()),
                    Err(_) => continue,
                },
            };

            let msg = match msg {
                Ok(Some(msg)) => msg,
                Ok(None) => continue,
                Err(err) => {
                    if let Some(msg) = err.to_error_message() {
                        let _ = self.send_message(msg).await;
                    }

                    break err.into();
                }
            };

            self.incoming_message_tx.send(Ok(msg)).await?;
        };

        self.incoming_message_tx.send(Err(err)).await
    }

    /// Process a given incoming message.
    async fn process_message(
        &mut self,
        msg: EncodedMessage,
    ) -> Result<Option<EncodedMessage>, ConnectionError> {
        let res = match msg.kind() {
            MessageKind::Ping => self.process_ping_message(msg).await,
            MessageKind::Pong => self.process_pong_message(msg).await,
            _ => return Ok(Some(msg)),
        };

        res.map(|_| None)
    }

    /// Process a given PING message.
    async fn process_ping_message(&mut self, msg: EncodedMessage) -> Result<(), ConnectionError> {
        debug_assert_eq!(msg.kind(), MessageKind::Ping);

        let payload = msg.data();

        let ping =
            PingMessage::decode(&mut payload.clone()).map_err(ConnectionError::InvalidMessage)?;

        let _ = self.send_message(PongMessage::new(ping.id())).await;

        Ok(())
    }

    /// Process a given PONG message.
    async fn process_pong_message(&mut self, msg: EncodedMessage) -> Result<(), ConnectionError> {
        debug_assert_eq!(msg.kind(), MessageKind::Pong);

        let payload = msg.data();

        let pong =
            PongMessage::decode(&mut payload.clone()).map_err(ConnectionError::InvalidMessage)?;

        let _ = self.incoming_pong_tx.send(pong.id()).await;

        Ok(())
    }

    /// Send a message.
    async fn send_message<T>(&mut self, msg: T) -> Result<(), SendError>
    where
        T: Message + EncodeMessage,
    {
        self.outgoing_message_tx
            .send(self.encoder.encode(&msg))
            .await
    }
}

/// Outgoing message sender.
type OutgoingMessageTx = mpsc::Sender<EncodedMessage>;

/// Outgoing message receiver.
type OutgoingMessageRx = mpsc::Receiver<EncodedMessage>;

/// Outgoing message sender.
struct OutgoingMessageSender {
    outgoing_message_rx: OutgoingMessageRx,
    outgoing_ping_rx: Abortable<OutgoingPingReceiver>,
}

impl OutgoingMessageSender {
    /// Forward all outgoing messages to a given sink.
    async fn send_all<T, E>(self, tx: T) -> Result<(), E>
    where
        T: Sink<EncodedMessage, Error = E>,
    {
        let mut encoder = MessageEncoder::new();

        let outgoing_ping_rx = self
            .outgoing_ping_rx
            .map(move |id| encoder.encode(&PingMessage::new(id)));

        let select = futures::stream::select(self.outgoing_message_rx, outgoing_ping_rx);

        OutgoingMessageStream::new(select).map(Ok).forward(tx).await
    }
}

pin_project_lite::pin_project! {
    /// Outgoing message stream that gets terminated after an error message.
    struct OutgoingMessageStream<T> {
        #[pin]
        inner: Option<T>,
    }
}

impl<T> OutgoingMessageStream<T> {
    /// Create a new outgoing message stream.
    fn new(inner: T) -> Self {
        Self { inner: Some(inner) }
    }
}

impl<T> Stream for OutgoingMessageStream<T>
where
    T: Stream<Item = EncodedMessage>,
{
    type Item = EncodedMessage;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        let inner = this.inner.as_mut();

        if let Some(stream) = inner.as_pin_mut() {
            let ready = ready!(stream.poll_next(cx));

            let close = ready
                .as_ref()
                .map(|msg| msg.kind() == MessageKind::Error)
                .unwrap_or(true);

            if close {
                this.inner.set(None);
            }

            Poll::Ready(ready)
        } else {
            Poll::Ready(None)
        }
    }
}

/// Connection error.
#[derive(Debug)]
enum ConnectionError {
    UnsupportedProtocolVersion(u8),
    UnknownMessageType(u8),
    PayloadSizeExceeded,
    InvalidMessage(Error),
    UnexpectedEOF,
    UnexpectedPongId(u16),
    Timeout,
    IO(io::Error),
}

impl ConnectionError {
    /// Create an error message that should be sent back to the remote peer.
    fn to_error_message(&self) -> Option<ErrorMessage> {
        let msg = match self {
            Self::UnsupportedProtocolVersion(_) => ErrorMessage::UnsupportedProtocolVersion,
            Self::UnknownMessageType(_) => ErrorMessage::UnknownMessageType,
            Self::PayloadSizeExceeded => ErrorMessage::PayloadSizeExceeded,
            Self::InvalidMessage(_) => ErrorMessage::InvalidMessage,
            Self::UnexpectedPongId(_) => ErrorMessage::UnexpectedPongId,
            _ => return None,
        };

        Some(msg)
    }
}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedProtocolVersion(v) => write!(f, "unsupported protocol version: {v}"),
            Self::UnknownMessageType(t) => write!(f, "unknown message type: {t:02x}"),
            Self::PayloadSizeExceeded => f.write_str("maximum payload size exceeded"),
            Self::InvalidMessage(err) => write!(f, "invalid message: {err}"),
            Self::UnexpectedEOF => f.write_str("unexpected EOF"),
            Self::UnexpectedPongId(id) => write!(f, "unexpected PONG ID: {id:04x}"),
            Self::Timeout => f.write_str("timeout"),
            Self::IO(err) => write!(f, "IO: {err}"),
        }
    }
}

impl std::error::Error for ConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IO(err) => Some(err),
            Self::InvalidMessage(err) => Some(err),
            _ => None,
        }
    }
}

impl From<PingPongError> for ConnectionError {
    fn from(err: PingPongError) -> Self {
        match err {
            PingPongError::UnexpectedPongId(id) => Self::UnexpectedPongId(id),
            PingPongError::Timeout => Self::Timeout,
        }
    }
}

impl From<BaseConnectionError> for ConnectionError {
    fn from(err: BaseConnectionError) -> Self {
        match err {
            BaseConnectionError::UnsupportedProtocolVersion(v) => {
                Self::UnsupportedProtocolVersion(v)
            }
            BaseConnectionError::UnknownMessageType(t) => Self::UnknownMessageType(t),
            BaseConnectionError::PayloadSizeExceeded => Self::PayloadSizeExceeded,
            BaseConnectionError::UnexpectedEOF => Self::UnexpectedEOF,
            BaseConnectionError::IO(err) => Self::IO(err),
        }
    }
}

impl From<io::Error> for ConnectionError {
    fn from(err: io::Error) -> Self {
        Self::IO(err)
    }
}

impl From<ConnectionError> for Error {
    fn from(err: ConnectionError) -> Self {
        Error::from_static_msg_and_cause("connection error", err)
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};

    use bytes::{Bytes, BytesMut};
    use futures::{SinkExt, StreamExt};

    use super::{Connection, ConnectionError};

    use crate::v3::{
        msg::MessageKind,
        utils::tests::{FakeIo, create_fake_io_input, create_fake_io_output, create_message},
    };

    #[tokio::test]
    async fn test_ping_handler_drop() {
        let msg = Bytes::from_static(&[0x03, 0x02, 0x00, 0x00, 0x00, 0x01, 0x01]);

        let (mut tx, rx) = create_fake_io_input(4);

        let io = FakeIo::new(rx, None);

        let (connection, ping_pong_handler) = Connection::builder()
            .with_max_rx_payload_size(1024)
            .build(io);

        std::mem::drop(ping_pong_handler);

        tokio::spawn(async move {
            // send a message after a short delay to check that the ping-pong
            // handler drop has no effect on the connection
            tokio::time::sleep(Duration::from_millis(100)).await;

            let _ = tx.send(Ok(msg)).await;
        });

        let messages = connection
            .filter_map(|res| futures::future::ready(res.ok()))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_unsupported_protocol_version() {
        let (mut incoming_tx, incoming_rx) = create_fake_io_input(4);
        let (outgoing_tx, outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        let (mut connection, ping_pong_handler) = Connection::builder()
            .with_max_rx_payload_size(1024)
            .build(io);

        let _ = incoming_tx
            .send(Ok(Bytes::from_static(&[0x00; 6][..])))
            .await;

        let err = connection
            .next()
            .await
            .expect("expected an item")
            .err()
            .expect("expected an error");

        let err = err
            .source()
            .and_then(|source| source.downcast_ref::<ConnectionError>());

        assert!(matches!(
            err,
            Some(ConnectionError::UnsupportedProtocolVersion(_))
        ));

        std::mem::drop(ping_pong_handler);

        let outgoing = outgoing_rx
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .fold(BytesMut::new(), |mut acc, chunk| {
                acc.extend_from_slice(&chunk);
                acc
            })
            .freeze();

        assert_eq!(outgoing, &[0x03, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00][..]);
    }

    #[tokio::test]
    async fn test_invalid_message() {
        let (mut incoming_tx, incoming_rx) = create_fake_io_input(4);
        let (outgoing_tx, outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        let (mut connection, ping_pong_handler) = Connection::builder()
            .with_max_rx_payload_size(1024)
            .build(io);

        // invalid PING message (payload too short)
        let msg = Bytes::from_static(&[0x03, 0x06, 0x00, 0x00, 0x00, 0x01, 0x00]);

        let _ = incoming_tx.send(Ok(msg)).await;

        let err = connection
            .next()
            .await
            .expect("expected an item")
            .err()
            .expect("expected an error");

        let err = err
            .source()
            .and_then(|source| source.downcast_ref::<ConnectionError>());

        assert!(matches!(err, Some(ConnectionError::InvalidMessage(_))));

        std::mem::drop(ping_pong_handler);

        let outgoing = outgoing_rx
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .fold(BytesMut::new(), |mut acc, chunk| {
                acc.extend_from_slice(&chunk);
                acc
            })
            .freeze();

        assert_eq!(outgoing, &[0x03, 0x02, 0x00, 0x00, 0x00, 0x01, 0x05][..]);
    }

    #[tokio::test]
    async fn test_ping_pong() {
        let (mut incoming_tx, incoming_rx) = create_fake_io_input(4);
        let (outgoing_tx, mut outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        let (mut connection, ping_pong_handler) = Connection::builder()
            .with_max_rx_payload_size(1024)
            .build(io);

        let ping_interval = Duration::from_millis(100);
        let pong_timeout = Duration::from_millis(200);

        let ping_pong_task = tokio::spawn(async move {
            ping_pong_handler.run(ping_interval, pong_timeout).await;
        });

        let ping_timeout = Duration::from_millis(500);

        let ping = tokio::time::timeout(ping_timeout, outgoing_rx.next())
            .await
            .expect("did not receive PING in time")
            .expect("outgoing channel closed");

        assert_eq!(ping, &[0x03, 0x06, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00][..]);

        let pong = Bytes::from_static(&[0x03, 0x07, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00]);

        let _ = incoming_tx.send(Ok(pong)).await;

        let ping = tokio::time::timeout(ping_timeout, outgoing_rx.next())
            .await
            .expect("did not receive PING in time")
            .expect("outgoing channel closed");

        assert_eq!(ping, &[0x03, 0x06, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01][..]);

        let connection_error_timeout = Duration::from_millis(500);

        let res = tokio::time::timeout(connection_error_timeout, connection.next())
            .await
            .expect("timeout waiting for connection error");

        if let Some(Err(err)) = res {
            let err = err
                .source()
                .and_then(|source| source.downcast_ref::<ConnectionError>());

            assert!(matches!(err, Some(ConnectionError::Timeout)));
        } else {
            panic!("expected a connection error");
        }

        let ping_pong_task_join_timeout = Duration::from_millis(500);

        tokio::time::timeout(ping_pong_task_join_timeout, ping_pong_task)
            .await
            .expect("timeout waiting for ping pong task to finish")
            .expect("ping pong task panicked");
    }

    #[tokio::test]
    async fn test_ping_response() {
        let (mut incoming_tx, incoming_rx) = create_fake_io_input(4);
        let (outgoing_tx, outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        let (mut connection, ping_pong_handler) = Connection::builder()
            .with_max_rx_payload_size(1024)
            .build(io);

        std::mem::drop(ping_pong_handler);

        let msg = Bytes::from_static(&[0x03, 0x06, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00]);

        let _ = incoming_tx.send(Ok(msg)).await;

        std::mem::drop(incoming_tx);

        let item = connection.next().await;

        assert!(item.is_none());

        std::mem::drop(connection);

        let outgoing = outgoing_rx
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .fold(BytesMut::new(), |mut acc, chunk| {
                acc.extend_from_slice(&chunk);
                acc
            })
            .freeze();

        assert_eq!(
            outgoing,
            &[0x03, 0x07, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00][..]
        );
    }

    #[tokio::test]
    async fn test_close_after_error_message() {
        let (incoming_tx, incoming_rx) = create_fake_io_input(4);
        let (outgoing_tx, outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        let (mut connection, ping_pong_handler) = Connection::builder()
            .with_max_rx_payload_size(1024)
            .build(io);

        std::mem::drop(ping_pong_handler);

        connection
            .send(create_message(MessageKind::Error, &[0x01]))
            .await
            .unwrap();

        connection
            .send(create_message(MessageKind::Error, &[0x07]))
            .await
            .unwrap();

        std::mem::drop(connection);

        let outgoing = outgoing_rx
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .fold(BytesMut::new(), |mut acc, chunk| {
                acc.extend_from_slice(&chunk);
                acc
            })
            .freeze();

        assert_eq!(outgoing, &[0x03, 0x02, 0x00, 0x00, 0x00, 0x01, 0x01][..]);

        std::mem::drop(incoming_tx);
    }
}
