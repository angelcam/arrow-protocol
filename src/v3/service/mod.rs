mod context;
mod error;
mod internal;
mod msg;

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::{Sink, Stream, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::JoinHandle,
};

use self::{
    context::ServiceConnectionContext,
    error::ServiceProtocolError,
    internal::{InternalConnection, InternalServerHandshake},
    msg::ServiceConnectionMessage,
};

use crate::v3::{error::Error, msg::error::ErrorMessage};

/// Service protocol connection builder.
pub struct ServiceProtocolConnectionBuilder {
    max_rx_payload_size: u32,
    rx_capacity: u32,
    ping_interval: Duration,
    pong_timeout: Duration,
}

impl ServiceProtocolConnectionBuilder {
    /// Create a new service connection builder.
    const fn new() -> Self {
        Self {
            max_rx_payload_size: 65536,
            rx_capacity: 65536,
            ping_interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(10),
        }
    }

    /// Set the maximum payload size for incoming messages.
    pub const fn with_max_rx_payload_size(mut self, size: u32) -> Self {
        self.max_rx_payload_size = size;
        self
    }

    /// Set the maximum amount of unacknowledged incoming data.
    pub const fn with_rx_capacity(mut self, size: u32) -> Self {
        self.rx_capacity = size;
        self
    }

    /// Set the PING interval.
    pub const fn with_ping_interval(mut self, interval: Duration) -> Self {
        self.ping_interval = interval;
        self
    }

    /// Set the PONG timeout.
    pub const fn with_pong_timeout(mut self, timeout: Duration) -> Self {
        self.pong_timeout = timeout;
        self
    }

    /// Build the service connection.
    pub async fn accept<T>(self, io: T) -> Result<ServiceProtocolHandshake, Error>
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let handshake = InternalConnection::builder()
            .with_max_rx_payload_size(self.max_rx_payload_size)
            .with_rx_capacity(self.rx_capacity)
            .with_ping_interval(self.ping_interval)
            .with_pong_timeout(self.pong_timeout)
            .accept(io)
            .await?;

        let res = ServiceProtocolHandshake {
            inner: handshake,
            rx_capacity: self.rx_capacity,
        };

        Ok(res)
    }

    /// Build the service connection.
    pub async fn connect<T, U>(
        self,
        io: T,
        access_token: U,
    ) -> Result<ServiceProtocolConnection, Error>
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
        U: Into<String>,
    {
        let connection = InternalConnection::builder()
            .with_max_rx_payload_size(self.max_rx_payload_size)
            .with_rx_capacity(self.rx_capacity)
            .with_ping_interval(self.ping_interval)
            .with_pong_timeout(self.pong_timeout)
            .connect(io, access_token)
            .await?;

        Ok(ServiceProtocolConnection::new(connection, self.rx_capacity))
    }
}

/// Service protocol server handshake.
pub struct ServiceProtocolHandshake {
    inner: InternalServerHandshake,
    rx_capacity: u32,
}

impl ServiceProtocolHandshake {
    /// Get the access token provided by the client.
    pub fn access_token(&self) -> &str {
        self.inner.access_token()
    }

    /// Accept the connection.
    pub async fn accept(self) -> Result<ServiceProtocolConnection, Error> {
        let connection = self.inner.accept().await?;

        Ok(ServiceProtocolConnection::new(connection, self.rx_capacity))
    }

    /// Reject the connection as unauthorized.
    pub async fn unauthorized(self) -> Result<(), Error> {
        self.inner.reject(ErrorMessage::Unauthorized).await
    }
}

/// Service protocol connection.
pub struct ServiceProtocolConnection {
    context: Arc<Mutex<ServiceConnectionContext>>,
    reader: JoinHandle<()>,
}

impl ServiceProtocolConnection {
    /// Create a new service protocol connection.
    fn new(connection: InternalConnection, rx_capacity: u32) -> Self {
        let remote_options = connection.remote_options();

        let max_tx_payload_size = remote_options.max_payload_size();
        let tx_capacity = remote_options.max_unacknowledged_data();

        let context = ServiceConnectionContext::new(
            rx_capacity as usize,
            tx_capacity as usize,
            max_tx_payload_size as usize,
        );

        let context = Arc::new(Mutex::new(context));

        let (tx, rx) = connection.split();

        let reader = IncomingMessageReader {
            context: context.clone(),
        };

        let reader = tokio::spawn(reader.read_all(rx));

        let sender = OutgoingMessageSender {
            context: context.clone(),
        };

        tokio::spawn(sender.send_all(tx));

        Self { context, reader }
    }

    /// Get a service connection builder.
    pub fn builder() -> ServiceProtocolConnectionBuilder {
        ServiceProtocolConnectionBuilder::new()
    }
}

impl Drop for ServiceProtocolConnection {
    fn drop(&mut self) {
        // stop the reader task
        self.reader.abort();

        // ... and terminate the connection context (this will allow the writer
        // task to write any remaining messages and exit)
        self.context.lock().unwrap().terminate();
    }
}

impl Stream for ServiceProtocolConnection {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.context.lock().unwrap().poll_incoming(cx)
    }
}

impl Sink<Bytes> for ServiceProtocolConnection {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.context.lock().unwrap().poll_ready_outgoing(cx)
    }

    fn start_send(self: Pin<&mut Self>, chunk: Bytes) -> Result<(), Self::Error> {
        self.context.lock().unwrap().start_send_outgoing(chunk)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.context.lock().unwrap().poll_flush_outgoing(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.context.lock().unwrap().poll_close_outgoing(cx)
    }
}

/// Incoming message reader.
struct IncomingMessageReader {
    context: Arc<Mutex<ServiceConnectionContext>>,
}

impl IncomingMessageReader {
    /// Read and process all incoming messages.
    async fn read_all<T>(self, rx: T)
    where
        T: Stream<Item = Result<ServiceConnectionMessage, ServiceProtocolError>>,
    {
        futures::pin_mut!(rx);

        while let Some(item) = rx.next().await {
            let mut context = self.context.lock().unwrap();

            context.process_incoming_item(item);

            if context.incoming_eof() {
                break;
            }
        }
    }
}

impl Drop for IncomingMessageReader {
    fn drop(&mut self) {
        self.context.lock().unwrap().close_incoming();
    }
}

/// Outgoing message sender.
struct OutgoingMessageSender {
    context: Arc<Mutex<ServiceConnectionContext>>,
}

impl OutgoingMessageSender {
    /// Send all outgoing messages.
    async fn send_all<T, E>(self, tx: T) -> Result<(), E>
    where
        T: Sink<ServiceConnectionMessage, Error = E>,
    {
        let stream = OutgoingMessageStream {
            context: self.context,
        };

        stream.map(Ok).forward(tx).await
    }
}

/// Stream of outgoing messages.
struct OutgoingMessageStream {
    context: Arc<Mutex<ServiceConnectionContext>>,
}

impl Stream for OutgoingMessageStream {
    type Item = ServiceConnectionMessage;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.context.lock().unwrap().poll_outgoing(cx)
    }
}

impl Drop for OutgoingMessageStream {
    fn drop(&mut self) {
        self.context.lock().unwrap().close_outgoing();
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};

    use bytes::Bytes;
    use futures::{SinkExt, StreamExt};

    use crate::v3::{
        msg::raw::{RawDataAckMessage, RawDataMessage},
        utils::tests::{
            EncodedMessageExt, FakeIo, MessageExt, create_fake_io_input, create_fake_io_output,
        },
    };

    use super::{ServiceProtocolConnection, ServiceProtocolError};

    /// Create an encoded raw data message.
    fn create_raw_data_message(data: &[u8]) -> Bytes {
        RawDataMessage::new(Bytes::copy_from_slice(data))
            .to_encoded_message()
            .to_bytes()
    }

    #[tokio::test]
    async fn test_rx_capacity_exceeded() {
        let (mut incoming_tx, incoming_rx) = create_fake_io_input(8);
        let (outgoing_tx, mut outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        let remote_options = &[
            0x03, 0x05, 0x00, 0x00, 0x00, 0x08, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        ];

        incoming_tx
            .send(Ok(Bytes::from_static(remote_options)))
            .await
            .unwrap();

        incoming_tx
            .send(Ok(create_raw_data_message(&[0x00; 32][..])))
            .await
            .unwrap();

        let mut connection = ServiceProtocolConnection::builder()
            .with_max_rx_payload_size(1024)
            .with_rx_capacity(32)
            .with_ping_interval(Duration::from_secs(20))
            .with_pong_timeout(Duration::from_secs(10))
            .connect(io, "foo")
            .await
            .unwrap();

        // get the first chunk in order to release some channel capacity
        connection.next().await.unwrap().unwrap();

        // try sending one more byte to verify that more capacity is available
        incoming_tx
            .send(Ok(create_raw_data_message(&[0x00][..])))
            .await
            .unwrap();

        // now exceed the capacity
        incoming_tx
            .send(Ok(create_raw_data_message(&[0x00; 32][..])))
            .await
            .unwrap();

        // we should be able to receive the second chunk
        let chunk = connection.next().await.unwrap().unwrap();

        assert_eq!(chunk.len(), 1);

        // ... and a part of the third chunk
        let chunk = connection.next().await.unwrap().unwrap();

        assert_eq!(chunk.len(), 31);

        // but we'll get an error on the next read because the third chunk
        // exceeded the capacity
        let res = connection.next().await;

        if let Some(Err(err)) = res {
            let err = err
                .source()
                .and_then(|source| source.downcast_ref::<ServiceProtocolError>());

            assert!(matches!(
                err,
                Some(ServiceProtocolError::ChannelCapacityExceeded)
            ));
        } else {
            panic!("expected a service connection error");
        }

        // skip the hello message and the local options message
        outgoing_rx.next().await.unwrap();
        outgoing_rx.next().await.unwrap();

        let ack_message = outgoing_rx.next().await.unwrap();

        assert_eq!(
            ack_message,
            &[0x03, 0x21, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x20][..]
        );

        let error_message = outgoing_rx.next().await.unwrap();

        assert_eq!(
            error_message,
            &[0x03, 0x02, 0x00, 0x00, 0x00, 0x01, 0x09][..]
        );

        assert!(outgoing_rx.next().await.is_none());
    }

    #[tokio::test]
    async fn test_tx_capacity_limit() {
        let (mut incoming_tx, incoming_rx) = create_fake_io_input(4);
        let (outgoing_tx, outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        let remote_options = &[
            0x03, 0x05, 0x00, 0x00, 0x00, 0x08, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
        ];

        incoming_tx
            .send(Ok(Bytes::from_static(remote_options)))
            .await
            .unwrap();

        let mut connection = ServiceProtocolConnection::builder()
            .with_max_rx_payload_size(1024)
            .with_rx_capacity(32)
            .with_ping_interval(Duration::from_secs(20))
            .with_pong_timeout(Duration::from_secs(10))
            .connect(io, "foo")
            .await
            .unwrap();

        tokio::spawn(async move {
            let _ = connection.send(Bytes::from(&[0x00; 64][..])).await;
        });

        // wait a bit before releasing channel capacity
        tokio::time::sleep(Duration::from_millis(100)).await;

        let ack = RawDataAckMessage::new(16).to_encoded_message().to_bytes();

        incoming_tx.send(Ok(ack)).await.unwrap();

        // wait another bit to make sure the sender forwards more data
        tokio::time::sleep(Duration::from_millis(100)).await;

        std::mem::drop(incoming_tx);

        let sent_messages: Vec<Bytes> =
            tokio::time::timeout(Duration::from_secs(1), outgoing_rx.collect())
                .await
                .unwrap();

        assert_eq!(sent_messages.len(), 4);

        assert_eq!(sent_messages[0][1], 0x01); // hello message
        assert_eq!(sent_messages[1][1], 0x05); // local options

        assert_eq!(sent_messages[2][1], 0x20); // first data message
        assert_eq!(&sent_messages[2][2..6], &u32::to_be_bytes(32)); // first data message length

        assert_eq!(sent_messages[3][1], 0x20); // second data message
        assert_eq!(&sent_messages[3][2..6], &u32::to_be_bytes(16)); // second data message length
    }
}
