pub mod error;
pub mod hello;
pub mod json;
pub mod options;
pub mod ping;
pub mod raw;
pub mod redirect;

use std::fmt::{self, Display, Formatter};

use bytes::{Bytes, BytesMut};

use crate::v3::{PROTOCOL_VERSION, error::Error};

/// Unknown message kind error.
#[derive(Debug)]
pub struct UnknownMessageKind(());

impl UnknownMessageKind {
    /// Create a new unknown message kind error.
    const fn new() -> Self {
        Self(())
    }
}

impl Display for UnknownMessageKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("unknown message kind")
    }
}

impl std::error::Error for UnknownMessageKind {}

/// Message kind.
#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum MessageKind {
    ControlProtocolHello = 0x00,
    ServiceProtocolHello = 0x01,
    Error = 0x02,
    Redirect = 0x03,
    ControlProtocolOptions = 0x04,
    ServiceProtocolOptions = 0x05,
    Ping = 0x06,
    Pong = 0x07,
    JsonRpcRequest = 0x10,
    JsonRpcResponse = 0x11,
    JsonRpcNotification = 0x12,
    RawData = 0x20,
    RawDataAck = 0x21,
}

impl TryFrom<u8> for MessageKind {
    type Error = UnknownMessageKind;

    fn try_from(value: u8) -> Result<Self, <Self as TryFrom<u8>>::Error> {
        match value {
            0x00 => Ok(MessageKind::ControlProtocolHello),
            0x01 => Ok(MessageKind::ServiceProtocolHello),
            0x02 => Ok(MessageKind::Error),
            0x03 => Ok(MessageKind::Redirect),
            0x04 => Ok(MessageKind::ControlProtocolOptions),
            0x05 => Ok(MessageKind::ServiceProtocolOptions),
            0x06 => Ok(MessageKind::Ping),
            0x07 => Ok(MessageKind::Pong),
            0x10 => Ok(MessageKind::JsonRpcRequest),
            0x11 => Ok(MessageKind::JsonRpcResponse),
            0x12 => Ok(MessageKind::JsonRpcNotification),
            0x20 => Ok(MessageKind::RawData),
            0x21 => Ok(MessageKind::RawDataAck),
            _ => Err(UnknownMessageKind::new()),
        }
    }
}

/// Decode message trait.
pub trait DecodeMessage {
    /// Decode the message.
    fn decode(encoded: &EncodedMessage) -> Result<Self, Error>
    where
        Self: Sized;
}

/// Encode message trait.
pub trait EncodeMessage {
    /// Encode the message and return the resulting payload bytes.
    ///
    /// The method can use the provided buffer to serialize the payload. This
    /// will help with reducing the number of allocations. The buffer will
    /// always be empty when the method is called.
    fn encode(&self, buf: &mut BytesMut) -> EncodedMessage;
}

/// Encoded message.
pub struct EncodedMessage {
    protocol_version: u8,
    kind: MessageKind,
    payload: Bytes,
}

impl EncodedMessage {
    /// Create a new encoded message.
    pub const fn new(kind: MessageKind, payload: Bytes) -> Self {
        assert!(payload.len() <= (u32::MAX as usize));

        Self::new_with_version(PROTOCOL_VERSION, kind, payload)
    }

    /// Create a new encoded message.
    pub(crate) const fn new_with_version(
        protocol_version: u8,
        kind: MessageKind,
        payload: Bytes,
    ) -> Self {
        Self {
            protocol_version,
            kind,
            payload,
        }
    }

    /// Get the protocol version.
    pub fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    /// Get the message kind.
    pub fn kind(&self) -> MessageKind {
        self.kind
    }

    /// Get the message payload.
    pub fn data(&self) -> &Bytes {
        &self.payload
    }
}

/// Message encoder.
#[derive(Default)]
pub struct MessageEncoder {
    buffer: BytesMut,
}

impl MessageEncoder {
    /// Create a new message encoder.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
        }
    }

    /// Encode a given message.
    pub fn encode<P>(&mut self, payload: &P) -> EncodedMessage
    where
        P: EncodeMessage,
    {
        self.buffer.clear();

        payload.encode(&mut self.buffer)
    }
}
