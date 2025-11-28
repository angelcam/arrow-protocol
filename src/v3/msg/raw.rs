use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::v3::{
    error::Error,
    msg::{DecodeMessage, EncodeMessage, Message, MessageKind},
};

/// Message carrying raw data.
pub struct RawDataMessage {
    data: Bytes,
}

impl RawDataMessage {
    /// Create a new Raw Data message.
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }

    /// Consume the message and get the raw data.
    pub fn into_data(self) -> Bytes {
        self.data
    }
}

impl Message for RawDataMessage {
    fn kind(&self) -> MessageKind {
        MessageKind::RawData
    }
}

impl DecodeMessage for RawDataMessage {
    fn decode(buf: &mut Bytes) -> Result<Self, Error> {
        Ok(Self::new(buf.split_to(buf.len())))
    }
}

impl EncodeMessage for RawDataMessage {
    fn encode(&self, _: &mut BytesMut) -> Bytes {
        self.data.clone()
    }
}

/// Message acknowledging received raw data.
pub struct RawDataAckMessage {
    length: u32,
}

impl RawDataAckMessage {
    /// Create a new Raw Data ACK message.
    pub const fn new(length: u32) -> Self {
        Self { length }
    }

    /// Get the acknowledged data length.
    pub fn length(&self) -> u32 {
        self.length
    }
}

impl Message for RawDataAckMessage {
    fn kind(&self) -> MessageKind {
        MessageKind::RawDataAck
    }
}

impl DecodeMessage for RawDataAckMessage {
    fn decode(buf: &mut Bytes) -> Result<Self, Error> {
        if buf.len() < std::mem::size_of::<u32>() {
            return Err(Error::from_static_msg("raw data ACK message too short"));
        }

        let res = Self {
            length: buf.get_u32(),
        };

        Ok(res)
    }
}

impl EncodeMessage for RawDataAckMessage {
    fn encode(&self, buf: &mut BytesMut) -> Bytes {
        buf.put_u32(self.length);

        let data = buf.split();

        data.freeze()
    }
}
