use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{Sink, SinkExt, Stream, StreamExt, ready};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::v3::{
    connection::{Connection, PingPongHandler},
    control::{
        error::{ControlProtocolConnectionError, ControlProtocolError},
        msg::ControlProtocolMessage,
    },
    error::Error,
    msg::{
        DecodeMessage, EncodeMessage, MessageEncoder, MessageKind, error::ErrorMessage,
        hello::ControlProtocolHelloMessage, options::ControlProtocolOptions,
    },
};

/// Server handshake.
pub struct InternalServerHandshake {
    connection: InternalConnection,
    ping_pong_handler: PingPongHandler,
    ping_interval: Duration,
    pong_timeout: Duration,
    client_hello: ControlProtocolHelloMessage,
    local_options: ControlProtocolOptions,
}

impl InternalServerHandshake {
    /// Create a new server handshake.
    pub(super) fn new(
        connection: Connection,
        ping_pong_handler: PingPongHandler,
        ping_interval: Duration,
        pong_timeout: Duration,
        client_hello: ControlProtocolHelloMessage,
        local_options: ControlProtocolOptions,
    ) -> Self {
        let remote_options = ControlProtocolOptions::new(65_536, 1);

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
    pub fn client_hello(&self) -> &ControlProtocolHelloMessage {
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

/// Builder for internal control protocol connections.
pub struct InternalConnectionBuilder {
    max_rx_payload_size: u32,
    max_local_concurrent_requests: u16,
    ping_interval: Duration,
    pong_timeout: Duration,
}

impl InternalConnectionBuilder {
    /// Get a new builder.
    const fn new() -> Self {
        Self {
            max_rx_payload_size: 65536,
            max_local_concurrent_requests: 16,
            ping_interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(10),
        }
    }

    /// Set the maximum payload size for incoming messages.
    pub const fn with_max_rx_payload_size(mut self, size: u32) -> Self {
        self.max_rx_payload_size = size;
        self
    }

    /// Set the maximum number of concurrent incoming requests.
    pub const fn with_max_local_concurrent_requests(mut self, count: u16) -> Self {
        self.max_local_concurrent_requests = count;
        self
    }

    /// Set the ping interval.
    pub const fn with_ping_interval(mut self, interval: Duration) -> Self {
        self.ping_interval = interval;
        self
    }

    /// Set the pong timeout.
    pub const fn with_pong_timeout(mut self, timeout: Duration) -> Self {
        self.pong_timeout = timeout;
        self
    }

    /// Accept a given connection.
    pub async fn accept<T>(self, io: T) -> Result<InternalServerHandshake, Error>
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (inner, ping_pong_handler) = Connection::builder()
            .with_max_rx_payload_size(self.max_rx_payload_size)
            .build(io);

        let remote_options = ControlProtocolOptions::new(65_536, 1);

        let mut connection = InternalConnection {
            inner,
            encoder: MessageEncoder::new(),
            remote_options,
        };

        let local_options = ControlProtocolOptions::new(
            self.max_rx_payload_size,
            self.max_local_concurrent_requests,
        );

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

    /// Connect using the provided IO stream.
    pub async fn connect<T>(
        self,
        io: T,
        hello: ControlProtocolHelloMessage,
    ) -> Result<InternalConnection, ControlProtocolConnectionError>
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (inner, ping_pong_handler) = Connection::builder()
            .with_max_rx_payload_size(self.max_rx_payload_size)
            .build(io);

        let remote_options = ControlProtocolOptions::new(65_536, 1);

        let mut res = InternalConnection {
            inner,
            encoder: MessageEncoder::new(),
            remote_options,
        };

        let local_options = ControlProtocolOptions::new(
            self.max_rx_payload_size,
            self.max_local_concurrent_requests,
        );

        res.client_handshake(hello, local_options).await?;

        tokio::spawn(ping_pong_handler.run(self.ping_interval, self.pong_timeout));

        Ok(res)
    }
}

/// Internal control protocol connection.
pub struct InternalConnection {
    inner: Connection,
    encoder: MessageEncoder,
    remote_options: ControlProtocolOptions,
}

impl InternalConnection {
    /// Get a builder for the internal control protocol connection.
    pub const fn builder() -> InternalConnectionBuilder {
        InternalConnectionBuilder::new()
    }

    /// Get the remote peer options.
    pub fn remote_options(&self) -> &ControlProtocolOptions {
        &self.remote_options
    }

    /// Perform the control protocol handshake.
    async fn client_handshake(
        &mut self,
        hello: ControlProtocolHelloMessage,
        local_options: ControlProtocolOptions,
    ) -> Result<(), ControlProtocolConnectionError> {
        let res = self.client_handshake_internal(hello, local_options).await;

        if let Err(err) = res.as_ref()
            && let Some(msg) = err.to_error_message()
        {
            let _ = self.send_message(msg).await;
        }

        match res {
            Ok(Some(msg)) => Err(msg.into()),
            Ok(None) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Perform the control protocol handshake.
    async fn client_handshake_internal(
        &mut self,
        hello: ControlProtocolHelloMessage,
        local_options: ControlProtocolOptions,
    ) -> Result<Option<ErrorMessage>, ControlProtocolError> {
        self.send_message(hello).await?;

        let response = self
            .read_message(MessageKind::ControlProtocolOptions)
            .await?;

        self.remote_options = match response {
            ReadMessageResult::ExpectedMessage(options) => options,
            ReadMessageResult::ErrorMessage(msg) => return Ok(Some(msg)),
        };

        self.send_message(local_options).await?;

        Ok(None)
    }

    /// Perform the service connection handshake.
    async fn start_server_handshake(&mut self) -> Result<ControlProtocolHelloMessage, Error> {
        let res = self
            .read_message(MessageKind::ControlProtocolHello)
            .await
            .and_then(|res| match res {
                ReadMessageResult::ExpectedMessage(msg) => Ok(msg),
                ReadMessageResult::ErrorMessage(msg) => Err(ControlProtocolError::Other(
                    Error::from_msg(format!("received error message: {msg}")),
                )),
            });

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
        local_options: ControlProtocolOptions,
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
        local_options: ControlProtocolOptions,
    ) -> Result<(), ControlProtocolError> {
        self.send_message(local_options).await?;

        self.remote_options = self
            .read_message(MessageKind::ControlProtocolOptions)
            .await
            .and_then(|res| match res {
                ReadMessageResult::ExpectedMessage(options) => Ok(options),
                ReadMessageResult::ErrorMessage(msg) => Err(ControlProtocolError::Other(
                    Error::from_msg(format!("received error message: {msg}")),
                )),
            })?;

        Ok(())
    }

    /// Read a message.
    async fn read_message<T>(
        &mut self,
        kind: MessageKind,
    ) -> Result<ReadMessageResult<T>, ControlProtocolError>
    where
        T: DecodeMessage,
    {
        let msg = self
            .inner
            .next()
            .await
            .ok_or_else(|| Error::from_static_msg("unexpected EOF"))
            .and_then(|res| res)
            .map_err(ControlProtocolError::Other)?;

        match msg.kind() {
            MessageKind::Error => {
                // NOTE: We don't send any error message back here, as the
                //   server is expected to close the connection after sending
                //   the error message. That's why we convert the decoding
                //   error into `ControlProtocolError::Other` here.
                ErrorMessage::decode(&msg)
                    .map(ReadMessageResult::ErrorMessage)
                    .map_err(ControlProtocolError::Other)
            }
            k if k == kind => T::decode(&msg)
                .map(ReadMessageResult::ExpectedMessage)
                .map_err(ControlProtocolError::InvalidMessage),
            k => Err(ControlProtocolError::UnexpectedMessageType(k)),
        }
    }

    /// Send a given control protocol message.
    async fn send_message<T>(&mut self, msg: T) -> Result<(), ControlProtocolError>
    where
        T: EncodeMessage,
    {
        self.inner
            .send(self.encoder.encode(&msg))
            .await
            .map_err(ControlProtocolError::Other)
    }
}

impl Stream for InternalConnection {
    type Item = Result<ControlProtocolMessage, ControlProtocolError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = ready!(self.inner.poll_next_unpin(cx))
            .transpose()
            .map_err(ControlProtocolError::Other)?
            .map(|msg| ControlProtocolMessage::decode(&msg));

        Poll::Ready(res)
    }
}

impl Sink<ControlProtocolMessage> for InternalConnection {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready_unpin(cx)
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        msg: ControlProtocolMessage,
    ) -> Result<(), Self::Error> {
        let msg = match msg {
            ControlProtocolMessage::JsonRpcRequest(m) => self.encoder.encode(&m),
            ControlProtocolMessage::JsonRpcResponse(m) => self.encoder.encode(&m),
            ControlProtocolMessage::JsonRpcNotification(m) => self.encoder.encode(&m),
            ControlProtocolMessage::Error(m) => self.encoder.encode(&m),
            ControlProtocolMessage::Redirect(m) => self.encoder.encode(&m),
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

/// Helper type.
enum ReadMessageResult<T> {
    ExpectedMessage(T),
    ErrorMessage(ErrorMessage),
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use futures::{SinkExt, StreamExt};

    use crate::{
        ClientId, MacAddr,
        v3::{
            msg::hello::ControlProtocolHelloMessage,
            utils::tests::{FakeIo, create_fake_io_input, create_fake_io_output},
        },
    };

    use super::InternalConnection;

    #[tokio::test]
    async fn test_successful_handshake() {
        let (mut incoming_tx, incoming_rx) = create_fake_io_input(4);
        let (outgoing_tx, mut outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        let remote_options = &[
            0x03, 0x04, 0x00, 0x00, 0x00, 0x06, 0x00, 0x01, 0x00, 0x00, 0x00, 0x10,
        ];

        incoming_tx
            .send(Ok(Bytes::from_static(remote_options)))
            .await
            .unwrap();

        let client_id = ClientId::from_bytes([0xaa; 16]);
        let client_key = [0xbb; 16];
        let client_mac = MacAddr::from([0xcc; 6]);

        let hello = ControlProtocolHelloMessage::new(client_id, client_key, client_mac);

        let connection = InternalConnection::builder()
            .with_max_rx_payload_size(1024)
            .with_max_local_concurrent_requests(4)
            .with_ping_interval(Duration::from_secs(20))
            .with_pong_timeout(Duration::from_secs(10))
            .connect(io, hello)
            .await
            .unwrap();

        let remote_options = connection.remote_options();

        assert_eq!(remote_options.max_payload_size(), 65536);
        assert_eq!(remote_options.max_concurrent_requests(), 16);

        std::mem::drop(connection);

        let hello = outgoing_rx.next().await.unwrap();

        let mut expected_hello = Vec::new();

        expected_hello.extend_from_slice(&[0x03, 0x00, 0x00, 0x00, 0x00, 0x2b]);
        expected_hello.extend_from_slice(&[0xaa; 16]); // client ID
        expected_hello.extend_from_slice(&[0xbb; 16]); // client key
        expected_hello.extend_from_slice(&[0xcc; 6]); // client MAC
        expected_hello.extend_from_slice(&[0u8; 4]); // flags
        expected_hello.extend_from_slice(&[0u8]); // extended info

        assert_eq!(hello, &expected_hello);

        let local_options = outgoing_rx.next().await.unwrap();

        let mut expected_local_options = Vec::new();

        expected_local_options.extend_from_slice(&[0x03, 0x04, 0x00, 0x00, 0x00, 0x06]);
        expected_local_options.extend_from_slice(&[0x00, 0x00, 0x04, 0x00, 0x00, 0x04]);

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

        let client_id = ClientId::from_bytes([0xaa; 16]);
        let client_key = [0xbb; 16];
        let client_mac = MacAddr::from([0xcc; 6]);

        let hello = ControlProtocolHelloMessage::new(client_id, client_key, client_mac);

        InternalConnection::builder()
            .with_max_rx_payload_size(1024)
            .with_max_local_concurrent_requests(4)
            .with_ping_interval(Duration::from_secs(20))
            .with_pong_timeout(Duration::from_secs(10))
            .connect(io, hello)
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

        // Then we try to send an unexpected non-control protocol message.
        let unexpected_msg = &[0x03, 0x20, 0x00, 0x00, 0x00, 0x01, 0x00];

        incoming_tx
            .send(Ok(Bytes::from_static(unexpected_msg)))
            .await
            .unwrap();

        let client_id = ClientId::from_bytes([0xaa; 16]);
        let client_key = [0xbb; 16];
        let client_mac = MacAddr::from([0xcc; 6]);

        let hello = ControlProtocolHelloMessage::new(client_id, client_key, client_mac);

        InternalConnection::builder()
            .with_max_rx_payload_size(1024)
            .with_max_local_concurrent_requests(4)
            .with_ping_interval(Duration::from_secs(20))
            .with_pong_timeout(Duration::from_secs(10))
            .connect(io, hello)
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

        let invalid_msg = &[0x03, 0x04, 0x00, 0x00, 0x00, 0x04, 0x00, 0x01, 0x00, 0x00];

        incoming_tx
            .send(Ok(Bytes::from_static(invalid_msg)))
            .await
            .unwrap();

        let client_id = ClientId::from_bytes([0xaa; 16]);
        let client_key = [0xbb; 16];
        let client_mac = MacAddr::from([0xcc; 6]);

        let hello = ControlProtocolHelloMessage::new(client_id, client_key, client_mac);

        InternalConnection::builder()
            .with_max_rx_payload_size(1024)
            .with_max_local_concurrent_requests(4)
            .with_ping_interval(Duration::from_secs(20))
            .with_pong_timeout(Duration::from_secs(10))
            .connect(io, hello)
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
