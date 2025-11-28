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

use futures::{Sink, Stream, StreamExt, stream::SplitStream};
use serde_lite::Serialize;
use tokio::io::{AsyncRead, AsyncWrite};

use self::{
    context::{ConnectionContext, OutgoingNotification, OutgoingRequest},
    error::ControlProtocolError,
    internal::{InternalConnection, InternalServerHandshake},
    msg::ControlProtocolMessage,
};

use crate::{
    ClientId, ClientKey, MacAddr,
    v3::{
        error::Error,
        msg::{
            error::ErrorMessage,
            json::{
                JsonRpcError, JsonRpcMethod, JsonRpcNotification, JsonRpcParams, JsonRpcRequest,
                JsonRpcResponse, JsonRpcValue,
            },
        },
    },
};

pub use self::error::ControlProtocolConnectionError;

/// Control protocol connection builder.
pub struct ControlProtocolConnectionBuilder {
    max_rx_payload_size: u32,
    max_local_concurrent_requests: u16,
    ping_interval: Duration,
    pong_timeout: Duration,
}

impl ControlProtocolConnectionBuilder {
    /// Create a new builder.
    const fn new() -> Self {
        Self {
            max_rx_payload_size: 1024 * 1024,
            max_local_concurrent_requests: 16,
            ping_interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(10),
        }
    }

    /// Set the maximum payload size for incoming messages.
    pub fn max_rx_payload_size(mut self, size: u32) -> Self {
        self.max_rx_payload_size = size;
        self
    }

    /// Set the maximum number of concurrent incoming requests.
    pub fn max_local_concurrent_requests(mut self, count: u16) -> Self {
        self.max_local_concurrent_requests = count;
        self
    }

    /// Set the ping interval.
    pub fn ping_interval(mut self, interval: Duration) -> Self {
        self.ping_interval = interval;
        self
    }

    /// Set the pong timeout.
    pub fn pong_timeout(mut self, timeout: Duration) -> Self {
        self.pong_timeout = timeout;
        self
    }

    /// Accept a given control protocol connection.
    pub async fn accept<T>(self, io: T) -> Result<ControlProtocolHandshake, Error>
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let handshake = InternalConnection::builder()
            .with_max_rx_payload_size(self.max_rx_payload_size)
            .with_max_local_concurrent_requests(self.max_local_concurrent_requests)
            .with_ping_interval(self.ping_interval)
            .with_pong_timeout(self.pong_timeout)
            .accept(io)
            .await?;

        let res = ControlProtocolHandshake {
            inner: handshake,
            max_local_concurrent_requests: self.max_local_concurrent_requests,
        };

        Ok(res)
    }

    /// Connect using the provided IO stream.
    pub async fn connect<T>(
        self,
        client_id: ClientId,
        client_key: ClientKey,
        client_mac: MacAddr,
        io: T,
    ) -> Result<ControlProtocolConnection, ControlProtocolConnectionError>
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let connection = InternalConnection::builder()
            .with_max_rx_payload_size(self.max_rx_payload_size)
            .with_max_local_concurrent_requests(self.max_local_concurrent_requests)
            .with_ping_interval(self.ping_interval)
            .with_pong_timeout(self.pong_timeout)
            .connect(io, client_id, client_key, client_mac)
            .await?;

        let res = ControlProtocolConnection::new(connection, self.max_local_concurrent_requests);

        Ok(res)
    }
}

/// Control protocol server handshake.
pub struct ControlProtocolHandshake {
    inner: InternalServerHandshake,
    max_local_concurrent_requests: u16,
}

impl ControlProtocolHandshake {
    /// Get the client ID.
    pub fn client_id(&self) -> &ClientId {
        self.inner.client_id()
    }

    /// Get the client key.
    pub fn client_key(&self) -> &ClientKey {
        self.inner.client_key()
    }

    /// Get the client MAC address.
    pub fn client_mac(&self) -> &MacAddr {
        self.inner.client_mac()
    }

    /// Accept the connection.
    pub async fn accept(self) -> Result<ControlProtocolConnection, Error> {
        let connection = self.inner.accept().await?;

        let res = ControlProtocolConnection::new(connection, self.max_local_concurrent_requests);

        Ok(res)
    }

    /// Reject the connection as unauthorized.
    pub async fn unauthorized(self) -> Result<(), Error> {
        self.inner.reject(ErrorMessage::Unauthorized).await
    }
}

/// Control protocol connection.
pub struct ControlProtocolConnection {
    rx: SplitStream<InternalConnection>,
    context: Arc<Mutex<ConnectionContext>>,
}

impl ControlProtocolConnection {
    /// Create a new control protocol connection.
    fn new(connection: InternalConnection, max_local_concurrent_requests: u16) -> Self {
        let max_local_concurrent_requests = max_local_concurrent_requests as usize;

        let max_remote_concurrent_requests =
            connection.remote_options().max_concurrent_requests() as usize;

        let context = ConnectionContext::new(
            max_local_concurrent_requests,
            max_remote_concurrent_requests,
        );

        let (tx, rx) = connection.split();

        let context = Arc::new(Mutex::new(context));

        let message_sender = OutgoingMessageSender {
            context: context.clone(),
        };

        // TERMINATION: The task will be terminated when all outgoing messages
        //   have been sent.
        tokio::spawn(message_sender.send_all(tx));

        Self { rx, context }
    }

    /// Get a control protocol connection builder.
    pub const fn builder() -> ControlProtocolConnectionBuilder {
        ControlProtocolConnectionBuilder::new()
    }

    /// Get a control protocol connection handle.
    pub fn handle(&self) -> ControlProtocolConnectionHandle {
        ControlProtocolConnectionHandle {
            context: self.context.clone(),
        }
    }

    /// Process incoming messages.
    ///
    /// This method must be run to drive the connection, process incoming
    /// messages and dispatch them to the provided local service. The method
    /// must be run even if the service does nothing. The method returns when
    /// a redirect message is received or when an error occurs. The redirection
    /// target is returned on success.
    pub async fn process_incoming_messages<T>(
        self,
        local_service: T,
    ) -> Result<String, ControlProtocolConnectionError>
    where
        T: ControlProtocolService + Send + Clone + 'static,
    {
        let message_reader = IncomingMessageReader {
            context: self.context,
            service: local_service,
        };

        message_reader.read_all(self.rx).await
    }
}

/// Control protocol connection handle.
#[derive(Clone)]
pub struct ControlProtocolConnectionHandle {
    context: Arc<Mutex<ConnectionContext>>,
}

impl ControlProtocolConnectionHandle {
    /// Send a given JSON-RPC request to the remote peer.
    pub async fn send_request<M, T>(
        &mut self,
        method: M,
        params: T,
    ) -> Result<JsonRpcResponse, Error>
    where
        M: Into<JsonRpcMethod>,
        T: Serialize,
    {
        let method = method.into();

        let params = JsonRpcParams::new(params)?;

        let (request, rx) = OutgoingRequest::new(method, params);

        self.context.lock().unwrap().push_outgoing_request(request);

        rx.await
            .map_err(|_| Error::from_static_msg("connection closed"))
    }

    /// Send a given JSON-RPC notification to the remote peer.
    pub async fn send_notification<M, T>(&mut self, method: M, params: T) -> Result<(), Error>
    where
        M: Into<JsonRpcMethod>,
        T: Serialize,
    {
        let method = method.into();

        let params = JsonRpcParams::new(params)?;

        let (notification, rx) = OutgoingNotification::new(method, params);

        self.context
            .lock()
            .unwrap()
            .push_outgoing_notification(notification);

        rx.await
            .map_err(|_| Error::from_static_msg("connection closed"))
    }
}

/// Control protocol service.
#[trait_variant::make(Send)]
pub trait ControlProtocolService {
    /// Process a given JSON-RPC request.
    async fn handle_request(
        self,
        method: JsonRpcMethod,
        params: JsonRpcParams,
    ) -> Result<JsonRpcValue, JsonRpcError>;

    /// Process a given JSON-RPC notification.
    fn handle_notification(self, _: JsonRpcMethod, _: JsonRpcParams)
    where
        Self: Sized,
    {
    }
}

/// Stream of outgoing messages.
struct OutgoingMessageStream {
    context: Arc<Mutex<ConnectionContext>>,
}

impl Stream for OutgoingMessageStream {
    type Item = ControlProtocolMessage;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.context.lock().unwrap().poll_outgoing_message(cx)
    }
}

/// Outgoing message sender.
struct OutgoingMessageSender {
    context: Arc<Mutex<ConnectionContext>>,
}

impl OutgoingMessageSender {
    /// Forward all outgoing messages.
    async fn send_all<T, E>(self, tx: T) -> Result<(), E>
    where
        T: Sink<ControlProtocolMessage, Error = E>,
    {
        let stream = OutgoingMessageStream {
            context: self.context,
        };

        stream.map(Ok).forward(tx).await
    }
}

/// Incoming message reader.
struct IncomingMessageReader<T> {
    context: Arc<Mutex<ConnectionContext>>,
    service: T,
}

impl<T> IncomingMessageReader<T>
where
    T: ControlProtocolService + Send + Clone + 'static,
{
    /// Read and process all incoming messages.
    async fn read_all<S>(self, rx: S) -> Result<String, ControlProtocolConnectionError>
    where
        S: Stream<Item = Result<ControlProtocolMessage, ControlProtocolError>>,
    {
        futures::pin_mut!(rx);

        loop {
            let res = match rx.next().await {
                Some(Ok(msg)) => match msg {
                    ControlProtocolMessage::JsonRpcRequest(request) => {
                        self.process_json_rpc_request(request)
                    }
                    ControlProtocolMessage::JsonRpcResponse(response) => {
                        self.process_json_rpc_response(response)
                    }
                    ControlProtocolMessage::JsonRpcNotification(notification) => {
                        self.process_json_rpc_notification(notification)
                    }
                    ControlProtocolMessage::Redirect(redirect) => {
                        return Ok(redirect.into_target());
                    }
                    ControlProtocolMessage::Error(err) => {
                        return Err(err.into());
                    }
                },
                Some(Err(err)) => Err(err),
                None => {
                    return Err(ControlProtocolConnectionError::Other(
                        Error::from_static_msg("connection lost"),
                    ));
                }
            };

            if let Err(err) = res {
                if let Some(msg) = err.to_error_message() {
                    self.context.lock().unwrap().send_error_message(msg);
                }

                return Err(err.into());
            }
        }
    }

    /// Process an incoming JSON-RPC request.
    fn process_json_rpc_request(
        &self,
        request: JsonRpcRequest,
    ) -> Result<(), ControlProtocolError> {
        let token = self.acquire_incoming_request_token()?;

        let service = self.service.clone();

        tokio::spawn(async move {
            let id = request.id();

            let (method, params) = request.deconstruct();

            let response = service
                .handle_request(method, params)
                .await
                .map(|result| JsonRpcResponse::success(id, result))
                .unwrap_or_else(|err| JsonRpcResponse::error(id, err));

            token.resolve(response);
        });

        Ok(())
    }

    /// Process an incoming JSON-RPC response.
    fn process_json_rpc_response(
        &self,
        response: JsonRpcResponse,
    ) -> Result<(), ControlProtocolError> {
        self.context
            .lock()
            .unwrap()
            .process_incoming_response(response)
    }

    /// Process an incoming JSON-RPC notification.
    fn process_json_rpc_notification(
        &self,
        notification: JsonRpcNotification,
    ) -> Result<(), ControlProtocolError> {
        let (method, params) = notification.deconstruct();

        self.service.clone().handle_notification(method, params);

        Ok(())
    }

    /// Acquire a token for an incoming request.
    fn acquire_incoming_request_token(&self) -> Result<IncomingRequestToken, ControlProtocolError> {
        let mut context = self.context.lock().unwrap();

        context.accept_incoming_request()?;

        let token = IncomingRequestToken {
            context: self.context.clone(),
        };

        Ok(token)
    }
}

impl<T> Drop for IncomingMessageReader<T> {
    fn drop(&mut self) {
        self.context.lock().unwrap().close();
    }
}

/// Incoming request token.
///
/// The token can be used as a handle to send back a response for the
/// associated incoming request. It also ensures that the number of concurrent
/// incoming requests is properly tracked.
struct IncomingRequestToken {
    context: Arc<Mutex<ConnectionContext>>,
}

impl IncomingRequestToken {
    /// Send back a given response.
    fn resolve(self, response: JsonRpcResponse) {
        self.context
            .lock()
            .unwrap()
            .push_outgoing_response(response);
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};

    use bytes::Bytes;
    use futures::{SinkExt, StreamExt};
    use serde_lite::Serialize;

    use crate::{
        ClientId, MacAddr,
        v3::{
            msg::json::{
                JsonRpcError, JsonRpcMethod, JsonRpcParams, JsonRpcRequest, JsonRpcResponse,
                JsonRpcValue,
            },
            utils::tests::{
                EncodedMessageExt, FakeIo, MessageExt, create_fake_io_input, create_fake_io_output,
            },
        },
    };

    use super::{ControlProtocolConnection, ControlProtocolError, ControlProtocolService};

    /// Helper type.
    #[derive(Clone)]
    struct DummyLocalService;

    impl ControlProtocolService for DummyLocalService {
        async fn handle_request(
            self,
            _: JsonRpcMethod,
            _: JsonRpcParams,
        ) -> Result<JsonRpcValue, JsonRpcError> {
            futures::future::pending().await
        }

        fn handle_notification(self, _: JsonRpcMethod, _: JsonRpcParams) {}
    }

    /// Create a JSON-RPC request.
    fn create_json_rpc_request<T>(id: u64, method: &'static str, params: T) -> JsonRpcRequest
    where
        T: Serialize,
    {
        let method = JsonRpcMethod::from(method);
        let params = JsonRpcParams::new(params);

        JsonRpcRequest::new(id, method, params.unwrap())
    }

    /// Create a JSON-RPC response.
    fn create_json_rpc_response<T>(id: u64, result: T) -> JsonRpcResponse
    where
        T: Serialize,
    {
        let result = result.serialize();

        JsonRpcResponse::success(id, result.unwrap())
    }

    #[tokio::test]
    async fn test_redirect() {
        let (mut incoming_tx, incoming_rx) = create_fake_io_input(4);
        let (outgoing_tx, outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        let remote_options = &[
            0x03, 0x04, 0x00, 0x00, 0x00, 0x06, 0x00, 0x01, 0x00, 0x00, 0x00, 0x10,
        ];

        incoming_tx
            .send(Ok(Bytes::from_static(remote_options)))
            .await
            .unwrap();

        let redirect = &[0x03, 0x03, 0x00, 0x00, 0x00, 0x04, b'f', b'o', b'o', 0x00];

        incoming_tx
            .send(Ok(Bytes::from_static(redirect)))
            .await
            .unwrap();

        let client_id = ClientId::from_bytes([0xaa; 16]);
        let client_key = [0xbb; 16];
        let client_mac = MacAddr::from([0xcc; 6]);

        let target = ControlProtocolConnection::builder()
            .max_rx_payload_size(1024)
            .max_local_concurrent_requests(4)
            .ping_interval(Duration::from_secs(20))
            .pong_timeout(Duration::from_secs(10))
            .connect(client_id, client_key, client_mac, io)
            .await
            .unwrap()
            .process_incoming_messages(DummyLocalService)
            .await
            .unwrap();

        assert_eq!(target, "foo");

        std::mem::drop(outgoing_rx);
    }

    #[tokio::test]
    async fn test_unexpected_message_response() {
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

        let unexpected_msg = &[0x03, 0x20, 0x00, 0x00, 0x00, 0x01, 0x00];

        incoming_tx
            .send(Ok(Bytes::from_static(unexpected_msg)))
            .await
            .unwrap();

        let client_id = ClientId::from_bytes([0xaa; 16]);
        let client_key = [0xbb; 16];
        let client_mac = MacAddr::from([0xcc; 6]);

        let err = ControlProtocolConnection::builder()
            .max_rx_payload_size(1024)
            .max_local_concurrent_requests(4)
            .ping_interval(Duration::from_secs(20))
            .pong_timeout(Duration::from_secs(10))
            .connect(client_id, client_key, client_mac, io)
            .await
            .unwrap()
            .process_incoming_messages(DummyLocalService)
            .await
            .err()
            .unwrap();

        let err = err
            .source()
            .and_then(|source| source.source())
            .and_then(|source| source.downcast_ref::<ControlProtocolError>());

        assert!(matches!(
            err,
            Some(&ControlProtocolError::UnexpectedMessageType(_))
        ));

        // skip the hello message and the local options message
        outgoing_rx.next().await.unwrap();
        outgoing_rx.next().await.unwrap();

        let error_message = outgoing_rx.next().await.unwrap();

        assert_eq!(
            error_message,
            &[0x03, 0x02, 0x00, 0x00, 0x00, 0x01, 0x04][..]
        );

        assert!(outgoing_rx.next().await.is_none());
    }

    #[tokio::test]
    async fn test_too_many_incoming_requests() {
        // helper struct
        #[derive(Serialize)]
        struct RequestParams;

        let (mut incoming_tx, incoming_rx) = create_fake_io_input(8);
        let (outgoing_tx, mut outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        let remote_options = &[
            0x03, 0x04, 0x00, 0x00, 0x00, 0x06, 0x00, 0x01, 0x00, 0x00, 0x00, 0x10,
        ];

        incoming_tx
            .send(Ok(Bytes::from_static(remote_options)))
            .await
            .unwrap();

        for id in 0..4 {
            let msg = create_json_rpc_request(id, "test_method", RequestParams)
                .to_encoded_message()
                .to_bytes();

            incoming_tx.send(Ok(msg)).await.unwrap();
        }

        let client_id = ClientId::from_bytes([0xaa; 16]);
        let client_key = [0xbb; 16];
        let client_mac = MacAddr::from([0xcc; 6]);

        let err = ControlProtocolConnection::builder()
            .max_rx_payload_size(1024)
            .max_local_concurrent_requests(2)
            .ping_interval(Duration::from_secs(20))
            .pong_timeout(Duration::from_secs(10))
            .connect(client_id, client_key, client_mac, io)
            .await
            .unwrap()
            .process_incoming_messages(DummyLocalService)
            .await
            .err()
            .unwrap();

        let err = err
            .source()
            .and_then(|source| source.source())
            .and_then(|source| source.downcast_ref::<ControlProtocolError>());

        assert!(matches!(
            err,
            Some(&ControlProtocolError::TooManyConcurrentRequests)
        ));

        // skip the hello message and the local options message
        outgoing_rx.next().await.unwrap();
        outgoing_rx.next().await.unwrap();

        let error_message = outgoing_rx.next().await.unwrap();

        assert_eq!(
            error_message,
            &[0x03, 0x02, 0x00, 0x00, 0x00, 0x01, 0x07][..]
        );

        assert!(outgoing_rx.next().await.is_none());
    }

    #[tokio::test]
    async fn test_outgoing_request_limit() {
        // helper struct
        #[derive(Serialize)]
        struct RequestParams;

        let (mut incoming_tx, incoming_rx) = create_fake_io_input(4);
        let (outgoing_tx, outgoing_rx) = create_fake_io_output();

        let io = FakeIo::new(incoming_rx, Some(outgoing_tx));

        let remote_options = &[
            0x03, 0x04, 0x00, 0x00, 0x00, 0x06, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04,
        ];

        incoming_tx
            .send(Ok(Bytes::from_static(remote_options)))
            .await
            .unwrap();

        let client_id = ClientId::from_bytes([0xaa; 16]);
        let client_key = [0xbb; 16];
        let client_mac = MacAddr::from([0xcc; 6]);

        let connection = ControlProtocolConnection::builder()
            .max_rx_payload_size(1024)
            .max_local_concurrent_requests(2)
            .ping_interval(Duration::from_secs(20))
            .pong_timeout(Duration::from_secs(10))
            .connect(client_id, client_key, client_mac, io)
            .await
            .unwrap();

        let handle = connection.handle();

        let connection_task = tokio::spawn(async move {
            connection
                .process_incoming_messages(DummyLocalService)
                .await
        });

        let responses = (0..8)
            .map(|_| {
                let mut handle = handle.clone();

                tokio::spawn(async move { handle.send_request("test_method", RequestParams).await })
            })
            .collect::<Vec<_>>();

        // XXX: We can't flush the outgoing messages here, so let's just wait
        //   a bit.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let response = create_json_rpc_response(0, RequestParams)
            .to_encoded_message()
            .to_bytes();

        incoming_tx.send(Ok(response)).await.unwrap();

        // This second wait is to ensure that another request slot is released
        //   after processing the first response. This will unlock sending
        //   one more request.
        tokio::time::sleep(Duration::from_millis(100)).await;

        std::mem::drop(incoming_tx);

        let err = connection_task.await.unwrap().err().unwrap().to_string();

        assert_eq!(err, "connection lost");

        let sent_messages: Vec<Bytes> = outgoing_rx.collect().await;

        assert_eq!(sent_messages.len(), 7);

        assert_eq!(sent_messages[0][1], 0x00); // hello message
        assert_eq!(sent_messages[1][1], 0x04); // local options

        for i in 2..7 {
            assert_eq!(sent_messages[i][1], 0x10); // JSON-RPC requests
        }

        futures::future::join_all(responses)
            .await
            .into_iter()
            .enumerate()
            .for_each(|(i, response)| {
                let response = response.unwrap();

                if i == 0 {
                    assert!(response.is_ok());
                } else {
                    assert!(response.is_err());
                }
            });
    }
}
