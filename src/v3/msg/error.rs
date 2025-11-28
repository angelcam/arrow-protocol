use std::fmt::{self, Display, Formatter};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::v3::{
    error::Error,
    msg::{DecodeMessage, EncodeMessage, Message, MessageKind},
};

/// Error message.
#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum ErrorMessage {
    UnsupportedProtocolVersion = 0x00,
    Unauthorized = 0x01,
    UnknownMessageType = 0x02,
    PayloadSizeExceeded = 0x03,
    UnexpectedMessageType = 0x04,
    InvalidMessage = 0x05,
    UnexpectedPongId = 0x06,
    TooManyConcurrentRequests = 0x07,
    UnexpectedResponseId = 0x08,
    ChannelCapacityExceeded = 0x09,
}

impl AsRef<str> for ErrorMessage {
    fn as_ref(&self) -> &str {
        match self {
            Self::UnsupportedProtocolVersion => "unsupported protocol version",
            Self::Unauthorized => "unauthorized",
            Self::UnknownMessageType => "unknown message type",
            Self::PayloadSizeExceeded => "maximum payload size exceeded",
            Self::UnexpectedMessageType => "unexpected message type",
            Self::InvalidMessage => "invalid message",
            Self::UnexpectedPongId => "unexpected PONG ID",
            Self::TooManyConcurrentRequests => "too many concurrent requests",
            Self::UnexpectedResponseId => "unexpected response ID",
            Self::ChannelCapacityExceeded => "channel capacity exceeded",
        }
    }
}

impl Display for ErrorMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl Message for ErrorMessage {
    fn kind(&self) -> MessageKind {
        MessageKind::Error
    }
}

impl DecodeMessage for ErrorMessage {
    fn decode(buf: &mut Bytes) -> Result<Self, Error> {
        if buf.is_empty() {
            Err(Error::from_static_msg("error message too short"))
        } else {
            Self::try_from(buf.get_u8())
        }
    }
}

impl EncodeMessage for ErrorMessage {
    fn encode(&self, buf: &mut BytesMut) -> Bytes {
        buf.put_u8(*self as u8);

        let data = buf.split();

        data.freeze()
    }
}

impl TryFrom<u8> for ErrorMessage {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let res = match value {
            0x00 => Self::UnsupportedProtocolVersion,
            0x01 => Self::Unauthorized,
            0x02 => Self::UnknownMessageType,
            0x03 => Self::PayloadSizeExceeded,
            0x04 => Self::UnexpectedMessageType,
            0x05 => Self::InvalidMessage,
            0x06 => Self::UnexpectedPongId,
            0x07 => Self::TooManyConcurrentRequests,
            0x08 => Self::UnexpectedResponseId,
            0x09 => Self::ChannelCapacityExceeded,
            _ => return Err(Error::from_static_msg("unknown error code")),
        };

        Ok(res)
    }
}
