use bytes::{Buf, BufMut, BytesMut};

use crate::v3::{
    error::Error,
    msg::{DecodeMessage, EncodeMessage, EncodedMessage, MessageKind},
};

/// Ping message.
pub struct PingMessage {
    id: u16,
}

impl PingMessage {
    /// Create a new Ping message.
    pub const fn new(id: u16) -> Self {
        Self { id }
    }

    /// Get the ping identifier.
    pub fn id(&self) -> u16 {
        self.id
    }
}

impl DecodeMessage for PingMessage {
    fn decode(encoded: &EncodedMessage) -> Result<Self, Error> {
        assert_eq!(encoded.kind(), MessageKind::Ping);

        let data = encoded.data();

        let mut buf = data.clone();

        if buf.len() < std::mem::size_of::<u16>() {
            return Err(Error::from_static_msg("PING message too short"));
        }

        let res = Self { id: buf.get_u16() };

        Ok(res)
    }
}

impl EncodeMessage for PingMessage {
    fn encode(&self, buf: &mut BytesMut) -> EncodedMessage {
        buf.put_u16(self.id);

        let data = buf.split();

        EncodedMessage::new(MessageKind::Ping, data.freeze())
    }
}

/// Pong message.
pub struct PongMessage {
    id: u16,
}

impl PongMessage {
    /// Create a new Pong message.
    pub const fn new(id: u16) -> Self {
        Self { id }
    }

    /// Get the pong identifier.
    pub fn id(&self) -> u16 {
        self.id
    }
}

impl DecodeMessage for PongMessage {
    fn decode(encoded: &EncodedMessage) -> Result<Self, Error> {
        assert_eq!(encoded.kind(), MessageKind::Pong);

        let data = encoded.data();

        let mut buf = data.clone();

        if buf.len() < std::mem::size_of::<u16>() {
            return Err(Error::from_static_msg("PONG message too short"));
        }

        let res = Self { id: buf.get_u16() };

        Ok(res)
    }
}

impl EncodeMessage for PongMessage {
    fn encode(&self, buf: &mut BytesMut) -> EncodedMessage {
        buf.put_u16(self.id);

        let data = buf.split();

        EncodedMessage::new(MessageKind::Pong, data.freeze())
    }
}
