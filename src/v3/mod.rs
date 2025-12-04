//! Arrow Protocol v3 implementation.

mod connection;
mod control;
mod error;
mod service;
mod utils;

pub mod msg;

use std::{
    fmt::{self, Display, Formatter},
    time::Duration,
};

use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};

use self::{
    connection::Connection,
    msg::{
        DecodeMessage, EncodeMessage, MessageEncoder, MessageKind,
        error::ErrorMessage,
        hello::{ControlProtocolHelloMessage, ServiceProtocolHelloMessage},
        options::{ControlProtocolOptions, ServiceProtocolOptions},
    },
};

pub use self::{
    control::{
        ControlProtocolClientConnection, ControlProtocolClientConnectionHandle,
        ControlProtocolConnectionBuilder, ControlProtocolConnectionError, ControlProtocolHandshake,
        ControlProtocolServerConnection, ControlProtocolServerConnectionHandle,
        ControlProtocolService,
    },
    error::Error,
    service::{
        ServiceProtocolConnection, ServiceProtocolConnectionBuilder, ServiceProtocolHandshake,
    },
};

const PROTOCOL_VERSION: u8 = 3;

/// Arrow protocol handshake.
pub enum ArrowProtocolHandshake {
    Control(ControlProtocolHandshake),
    Service(ServiceProtocolHandshake),
}

impl From<ControlProtocolHandshake> for ArrowProtocolHandshake {
    #[inline]
    fn from(handshake: ControlProtocolHandshake) -> Self {
        Self::Control(handshake)
    }
}

impl From<ServiceProtocolHandshake> for ArrowProtocolHandshake {
    #[inline]
    fn from(handshake: ServiceProtocolHandshake) -> Self {
        Self::Service(handshake)
    }
}

/// Arrow protocol acceptor.
///
/// The acceptor can be used to accept incoming Arrow Protocol connections in
/// situations when the server does not know in advance whether the client
/// wants to use the control protocol or the service protocol.
pub struct ArrowProtocolAcceptor {
    encoder: MessageEncoder,
    max_rx_payload_size: u32,
    max_local_concurrent_requests: u16,
    rx_capacity: u32,
    ping_interval: Duration,
    pong_timeout: Duration,
}

impl ArrowProtocolAcceptor {
    /// Create a new acceptor with default options.
    pub fn new() -> Self {
        Self {
            encoder: MessageEncoder::new(),
            max_rx_payload_size: 1 << 20,
            max_local_concurrent_requests: 16,
            rx_capacity: 1 << 16,
            ping_interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(10),
        }
    }

    /// Set the maximum payload size for incoming messages.
    #[inline]
    pub const fn with_max_rx_payload_size(mut self, size: u32) -> Self {
        self.max_rx_payload_size = size;
        self
    }

    /// Set the maximum number of concurrent incoming requests (for control
    /// protocol connections).
    #[inline]
    pub const fn with_max_local_concurrent_requests(mut self, count: u16) -> Self {
        self.max_local_concurrent_requests = count;
        self
    }

    /// Set the maximum amount of unacknowledged incoming data (for service
    /// protocol connections).
    #[inline]
    pub const fn with_rx_capacity(mut self, size: u32) -> Self {
        self.rx_capacity = size;
        self
    }

    /// Set the ping interval.
    #[inline]
    pub const fn with_ping_interval(mut self, interval: Duration) -> Self {
        self.ping_interval = interval;
        self
    }

    /// Set the pong timeout.
    #[inline]
    pub const fn with_pong_timeout(mut self, timeout: Duration) -> Self {
        self.pong_timeout = timeout;
        self
    }

    /// Accept a given connection.
    pub async fn accept<T>(&mut self, io: T) -> Result<ArrowProtocolHandshake, Error>
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (mut connection, ping_pong_handler) = Connection::builder()
            .with_max_rx_payload_size(self.max_rx_payload_size)
            .build(io);

        let res = self.accept_internal(&mut connection).await;

        if let Err(err) = res.as_ref()
            && let Some(msg) = err.to_error_message()
        {
            let _ = self.send_message(&mut connection, msg).await;
        }

        match res.map_err(Error::from)? {
            ClientHelloMessage::Control(client_hello) => {
                let local_options = ControlProtocolOptions::new(
                    self.max_rx_payload_size,
                    self.max_local_concurrent_requests,
                );

                let handshake = ControlProtocolHandshake::new(
                    connection,
                    ping_pong_handler,
                    self.ping_interval,
                    self.pong_timeout,
                    client_hello,
                    local_options,
                );

                Ok(handshake.into())
            }
            ClientHelloMessage::Service(client_hello) => {
                let local_options =
                    ServiceProtocolOptions::new(self.max_rx_payload_size, self.rx_capacity);

                let handshake = ServiceProtocolHandshake::new(
                    connection,
                    ping_pong_handler,
                    self.ping_interval,
                    self.pong_timeout,
                    client_hello,
                    local_options,
                );

                Ok(handshake.into())
            }
        }
    }

    /// Accept a given connection.
    async fn accept_internal(
        &mut self,
        connection: &mut Connection,
    ) -> Result<ClientHelloMessage, AcceptError> {
        let msg = connection
            .next()
            .await
            .ok_or_else(|| Error::from_static_msg("unexpected EOF"))
            .and_then(|res| res)
            .map_err(AcceptError::Other)?;

        match msg.kind() {
            MessageKind::ControlProtocolHello => ControlProtocolHelloMessage::decode(&msg)
                .map(ClientHelloMessage::Control)
                .map_err(AcceptError::InvalidMessage),
            MessageKind::ServiceProtocolHello => ServiceProtocolHelloMessage::decode(&msg)
                .map(ClientHelloMessage::Service)
                .map_err(AcceptError::InvalidMessage),
            MessageKind::Error => {
                // NOTE: We don't send any error message back here, as the
                //   server is expected to close the connection after sending
                //   the error message. That's why we return
                //   `AcceptError::Other` here.

                let err = ErrorMessage::decode(&msg)
                    .map(|msg| Error::from_msg(format!("received error message: {msg}")))
                    .unwrap_or_else(|err| err);

                Err(AcceptError::Other(err))
            }
            k => Err(AcceptError::UnexpectedMessageType(k)),
        }
    }

    /// Send a given message.
    async fn send_message<T>(
        &mut self,
        connection: &mut Connection,
        msg: T,
    ) -> Result<(), AcceptError>
    where
        T: EncodeMessage,
    {
        connection
            .send(self.encoder.encode(&msg))
            .await
            .map_err(AcceptError::Other)
    }
}

impl Default for ArrowProtocolAcceptor {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Accept error.
#[derive(Debug)]
enum AcceptError {
    UnexpectedMessageType(MessageKind),
    InvalidMessage(Error),
    Other(Error),
}

impl AcceptError {
    /// Get an error message to be sent over to the remote peer (if any).
    fn to_error_message(&self) -> Option<ErrorMessage> {
        let res = match self {
            Self::UnexpectedMessageType(_) => ErrorMessage::UnexpectedMessageType,
            Self::InvalidMessage(_) => ErrorMessage::InvalidMessage,
            Self::Other(_) => return None,
        };

        Some(res)
    }
}

impl Display for AcceptError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMessage(err) => write!(f, "invalid message: {err}"),
            Self::UnexpectedMessageType(t) => {
                write!(f, "unexpected message type: {:02x}", *t as u8)
            }
            Self::Other(err) => Display::fmt(err, f),
        }
    }
}

impl std::error::Error for AcceptError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidMessage(err) => Some(err),
            Self::Other(err) => Some(err),
            _ => None,
        }
    }
}

impl From<AcceptError> for Error {
    fn from(err: AcceptError) -> Self {
        if let AcceptError::Other(err) = err {
            err
        } else {
            Error::from_static_msg_and_cause("accept error", err)
        }
    }
}

/// Client hello message.
enum ClientHelloMessage {
    Control(ControlProtocolHelloMessage),
    Service(ServiceProtocolHelloMessage),
}
