use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{Sink, SinkExt, Stream, StreamExt, ready};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::v3::{
    connection::{Connection, PingPongHandler},
    error::Error,
    msg::{
        DecodeMessage, EncodeMessage, MessageEncoder, MessageKind, error::ErrorMessage,
        hello::ServiceProtocolHelloMessage, options::ServiceProtocolOptions,
    },
    service::{error::ServiceProtocolError, msg::ServiceConnectionMessage},
};

/// Server handshake.
pub struct InternalServerHandshake {
    connection: InternalConnection,
    ping_pong_handler: PingPongHandler,
    ping_interval: Duration,
    pong_timeout: Duration,
    client_hello: ServiceProtocolHelloMessage,
    local_options: ServiceProtocolOptions,
}

impl InternalServerHandshake {
    /// Create a new server handshake.
    pub(super) fn new(
        connection: Connection,
        ping_pong_handler: PingPongHandler,
        ping_interval: Duration,
        pong_timeout: Duration,
        client_hello: ServiceProtocolHelloMessage,
        local_options: ServiceProtocolOptions,
    ) -> Self {
        let remote_options = ServiceProtocolOptions::new(65_536, 65_536);

        let connection = InternalConnection {
            inner: connection,
            encoder: MessageEncoder::new(),
            remote_options,
        };

        Self {
            connection,
            ping_pong_handler,
            ping_interval,
            pong_timeout,
            client_hello,
            local_options,
        }
    }

    /// Get the client hello message.
    pub fn client_hello(&self) -> &ServiceProtocolHelloMessage {
        &self.client_hello
    }

    /// Accept the connection.
    pub async fn accept(mut self) -> Result<InternalConnection, Error> {
        self.connection
            .complete_server_handshake(self.local_options)
            .await?;

        tokio::spawn(
            self.ping_pong_handler
                .run(self.ping_interval, self.pong_timeout),
        );

        Ok(self.connection)
    }

    /// Reject the connection.
    pub async fn reject(mut self, err: ErrorMessage) -> Result<(), Error> {
        self.connection.send_message(err).await.map_err(Error::from)
    }
}

/// Internal service connection builder.
pub struct InternalConnectionBuilder {
    max_rx_payload_size: u32,
    rx_capacity: u32,
    ping_interval: Duration,
    pong_timeout: Duration,
}

impl InternalConnectionBuilder {
    /// Create a new connection builder.
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

    /// Build the connection.
    pub async fn accept<T>(self, io: T) -> Result<InternalServerHandshake, Error>
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (inner, ping_pong_handler) = Connection::builder()
            .with_max_rx_payload_size(self.max_rx_payload_size)
            .build(io);

        let local_options = ServiceProtocolOptions::new(self.max_rx_payload_size, self.rx_capacity);

        let remote_options = ServiceProtocolOptions::new(65_536, 65_536);

        let mut connection = InternalConnection {
            inner,
            encoder: MessageEncoder::new(),
            remote_options,
        };

        let client_hello = connection.start_server_handshake().await?;

        let handshake = InternalServerHandshake {
            connection,
            ping_pong_handler,
            ping_interval: self.ping_interval,
            pong_timeout: self.pong_timeout,
            client_hello,
            local_options,
        };

        Ok(handshake)
    }

    /// Build the connection.
    pub async fn connect<T>(
        self,
        io: T,
        hello: ServiceProtocolHelloMessage,
    ) -> Result<InternalConnection, Error>
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (inner, ping_pong_handler) = Connection::builder()
            .with_max_rx_payload_size(self.max_rx_payload_size)
            .build(io);

        let local_options = ServiceProtocolOptions::new(self.max_rx_payload_size, self.rx_capacity);

        let remote_options = ServiceProtocolOptions::new(65_536, 65_536);

        let mut res = InternalConnection {
            inner,
            encoder: MessageEncoder::new(),
            remote_options,
        };

        res.client_handshake(hello, local_options).await?;

        tokio::spawn(ping_pong_handler.run(self.ping_interval, self.pong_timeout));

        Ok(res)
    }
}

/// Internal service connection.
pub struct InternalConnection {
    inner: Connection,
    encoder: MessageEncoder,
    remote_options: ServiceProtocolOptions,
}

impl InternalConnection {
    /// Get a new connection builder.
    pub const fn builder() -> InternalConnectionBuilder {
        InternalConnectionBuilder::new()
    }

    /// Get the remote peer options.
    pub fn remote_options(&self) -> &ServiceProtocolOptions {
        &self.remote_options
    }

    /// Perform the service connection handshake.
    async fn client_handshake(
        &mut self,
        hello: ServiceProtocolHelloMessage,
        local_options: ServiceProtocolOptions,
    ) -> Result<(), Error> {
        let res = self.client_handshake_internal(hello, local_options).await;

        if let Err(err) = res.as_ref()
            && let Some(msg) = err.to_error_message()
        {
            let _ = self.send_message(msg).await;
        }

        res.map_err(Error::from)
    }

    /// Perform the service connection handshake.
    async fn client_handshake_internal(
        &mut self,
        hello: ServiceProtocolHelloMessage,
        local_options: ServiceProtocolOptions,
    ) -> Result<(), ServiceProtocolError> {
        self.send_message(hello).await?;

        self.remote_options = self
            .read_message(MessageKind::ServiceProtocolOptions)
            .await?;

        self.send_message(local_options).await
    }

    /// Perform the service connection handshake.
    async fn start_server_handshake(&mut self) -> Result<ServiceProtocolHelloMessage, Error> {
        let res = self.read_message(MessageKind::ServiceProtocolHello).await;

        if let Err(err) = res.as_ref()
            && let Some(msg) = err.to_error_message()
        {
            let _ = self.send_message(msg).await;
        }

        res.map_err(Error::from)
    }

    /// Perform the service connection handshake.
    async fn complete_server_handshake(
        &mut self,
        local_options: ServiceProtocolOptions,
    ) -> Result<(), Error> {
        let res = self.complete_server_handshake_internal(local_options).await;

        if let Err(err) = res.as_ref()
            && let Some(msg) = err.to_error_message()
        {
            let _ = self.send_message(msg).await;
        }

        res.map_err(Error::from)
    }

    /// Perform the service connection handshake.
    async fn complete_server_handshake_internal(
        &mut self,
        local_options: ServiceProtocolOptions,
    ) -> Result<(), ServiceProtocolError> {
        self.send_message(local_options).await?;

        self.remote_options = self
            .read_message(MessageKind::ServiceProtocolOptions)
            .await?;

        Ok(())
    }

    /// Read a message.
    async fn read_message<T>(&mut self, kind: MessageKind) -> Result<T, ServiceProtocolError>
    where
        T: DecodeMessage,
    {
        let msg = self
            .inner
            .next()
            .await
            .ok_or_else(|| Error::from_static_msg("unexpected EOF"))
            .and_then(|res| res)
            .map_err(ServiceProtocolError::Other)?;

        match msg.kind() {
            MessageKind::Error => {
                // NOTE: We don't send any error message back here, as the
                //   server is expected to close the connection after sending
                //   the error message. That's why we return
                //   `ServiceProtocolError::Other` here.

                let err = ErrorMessage::decode(&msg)
                    .map(|msg| Error::from_msg(format!("received error message: {msg}")))
                    .unwrap_or_else(|err| err);

                Err(ServiceProtocolError::Other(err))
            }
            k if k == kind => T::decode(&msg).map_err(ServiceProtocolError::InvalidMessage),
            k => Err(ServiceProtocolError::UnexpectedMessageType(k)),
        }
    }

    /// Send a given message.
    async fn send_message<T>(&mut self, msg: T) -> Result<(), ServiceProtocolError>
    where
        T: EncodeMessage,
    {
        self.inner
            .send(self.encoder.encode(&msg))
            .await
            .map_err(ServiceProtocolError::Other)
    }
}

impl Stream for InternalConnection {
    type Item = Result<ServiceConnectionMessage, ServiceProtocolError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = ready!(self.inner.poll_next_unpin(cx))
            .transpose()
            .map_err(ServiceProtocolError::Other)?
            .map(|msg| ServiceConnectionMessage::decode(&msg));

        Poll::Ready(res)
    }
}

impl Sink<ServiceConnectionMessage> for InternalConnection {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready_unpin(cx)
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        msg: ServiceConnectionMessage,
    ) -> Result<(), Self::Error> {
        let msg = match msg {
            ServiceConnectionMessage::Data(m) => self.encoder.encode(&m),
            ServiceConnectionMessage::DataAck(m) => self.encoder.encode(&m),
            ServiceConnectionMessage::Error(m) => self.encoder.encode(&m),
        };

        let data = msg.data();

        if data.len() > (self.remote_options.max_payload_size() as usize) {
            return Err(Error::from_static_msg(
                "maximum outgoing message size exceeded",
            ));
        }

        self.inner.start_send_unpin(msg)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_close_unpin(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use futures::{SinkExt, StreamExt};

    use crate::v3::{
        msg::hello::ServiceProtocolHelloMessage,
        utils::tests::{FakeIo, create_fake_io_input, create_fake_io_output},
    };

    use super::InternalConnection;

    #[tokio::test]
    async fn test_successful_handshake() {
        let (mut incoming_tx, incoming_rx) = create_fake_io_input(4);
        let (outgoing_tx, mut outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        let remote_options = &[
            0x03, 0x05, 0x00, 0x00, 0x00, 0x08, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        ];

        incoming_tx
            .send(Ok(Bytes::from_static(remote_options)))
            .await
            .unwrap();

        let connection = InternalConnection::builder()
            .with_max_rx_payload_size(1024)
            .with_rx_capacity(2048)
            .with_ping_interval(Duration::from_secs(20))
            .with_pong_timeout(Duration::from_secs(10))
            .connect(io, ServiceProtocolHelloMessage::new("foo"))
            .await
            .unwrap();

        let remote_options = connection.remote_options();

        assert_eq!(remote_options.max_payload_size(), 65536);
        assert_eq!(remote_options.max_unacknowledged_data(), 65536);

        std::mem::drop(connection);

        let hello = outgoing_rx.next().await.unwrap();

        let mut expected_hello = Vec::new();

        expected_hello.extend_from_slice(&[0x03, 0x01, 0x00, 0x00, 0x00, 0x04]);
        expected_hello.extend_from_slice(b"foo\0");

        assert_eq!(hello, &expected_hello);

        let local_options = outgoing_rx.next().await.unwrap();

        let mut expected_local_options = Vec::new();

        expected_local_options.extend_from_slice(&[0x03, 0x05, 0x00, 0x00, 0x00, 0x08]);
        expected_local_options.extend_from_slice(&[0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x08, 0x00]);

        assert_eq!(local_options, &expected_local_options);

        assert!(outgoing_rx.next().await.is_none());
    }

    #[tokio::test]
    async fn test_unexpected_handshake_response() {
        let (mut incoming_tx, incoming_rx) = create_fake_io_input(4);
        let (outgoing_tx, mut outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        // First we try to send an unexpected control protocol message.
        let unexpected_msg = &[0x03, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00];

        incoming_tx
            .send(Ok(Bytes::from_static(unexpected_msg)))
            .await
            .unwrap();

        InternalConnection::builder()
            .with_max_rx_payload_size(1024)
            .with_rx_capacity(1024)
            .with_ping_interval(Duration::from_secs(20))
            .with_pong_timeout(Duration::from_secs(10))
            .connect(io, ServiceProtocolHelloMessage::new("foo"))
            .await
            .err()
            .unwrap();

        // we skip the hello message
        outgoing_rx.next().await.unwrap();

        let error_message = outgoing_rx.next().await.unwrap();

        assert_eq!(
            error_message,
            &[0x03, 0x02, 0x00, 0x00, 0x00, 0x01, 0x04][..]
        );

        assert!(outgoing_rx.next().await.is_none());

        let (mut incoming_tx, incoming_rx) = create_fake_io_input(4);
        let (outgoing_tx, mut outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        // Then we try to send an unexpected non-service protocol message.
        let unexpected_msg = &[0x03, 0x10, 0x00, 0x00, 0x00, 0x00];

        incoming_tx
            .send(Ok(Bytes::from_static(unexpected_msg)))
            .await
            .unwrap();

        InternalConnection::builder()
            .with_max_rx_payload_size(1024)
            .with_rx_capacity(1024)
            .with_ping_interval(Duration::from_secs(20))
            .with_pong_timeout(Duration::from_secs(10))
            .connect(io, ServiceProtocolHelloMessage::new("foo"))
            .await
            .err()
            .unwrap();

        // we skip the hello message
        outgoing_rx.next().await.unwrap();

        let error_message = outgoing_rx.next().await.unwrap();

        assert_eq!(
            error_message,
            &[0x03, 0x02, 0x00, 0x00, 0x00, 0x01, 0x04][..]
        );

        assert!(outgoing_rx.next().await.is_none());
    }

    #[tokio::test]
    async fn test_invalid_handshake_response() {
        let (mut incoming_tx, incoming_rx) = create_fake_io_input(4);
        let (outgoing_tx, mut outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        let invalid_msg = &[
            0x03, 0x05, 0x00, 0x00, 0x00, 0x06, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        ];

        incoming_tx
            .send(Ok(Bytes::from_static(invalid_msg)))
            .await
            .unwrap();

        InternalConnection::builder()
            .with_max_rx_payload_size(1024)
            .with_max_rx_payload_size(1024)
            .with_ping_interval(Duration::from_secs(20))
            .with_pong_timeout(Duration::from_secs(10))
            .connect(io, ServiceProtocolHelloMessage::new("foo"))
            .await
            .err()
            .unwrap();

        // we skip the hello message
        outgoing_rx.next().await.unwrap();

        let error_message = outgoing_rx.next().await.unwrap();

        assert_eq!(
            error_message,
            &[0x03, 0x02, 0x00, 0x00, 0x00, 0x01, 0x05][..]
        );

        assert!(outgoing_rx.next().await.is_none());
    }
}
