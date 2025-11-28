use std::fmt::{self, Display, Formatter};

use crate::v3::{
    error::Error,
    msg::{MessageKind, error::ErrorMessage},
};

/// Service protocol error.
#[derive(Debug)]
pub enum ServiceProtocolError {
    UnexpectedMessageType(MessageKind),
    InvalidMessage(Error),
    ChannelCapacityExceeded,
    Other(Error),
}

impl ServiceProtocolError {
    /// Get an error message to be sent over to the remote peer (if any).
    pub fn to_error_message(&self) -> Option<ErrorMessage> {
        let res = match self {
            Self::UnexpectedMessageType(_) => ErrorMessage::UnexpectedMessageType,
            Self::InvalidMessage(_) => ErrorMessage::InvalidMessage,
            Self::ChannelCapacityExceeded => ErrorMessage::ChannelCapacityExceeded,
            Self::Other(_) => return None,
        };

        Some(res)
    }
}

impl Display for ServiceProtocolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMessage(err) => write!(f, "invalid message: {err}"),
            Self::UnexpectedMessageType(t) => {
                write!(f, "unexpected message type: {:02x}", *t as u8)
            }
            Self::ChannelCapacityExceeded => f.write_str("channel capacity exceeded"),
            Self::Other(err) => Display::fmt(err, f),
        }
    }
}

impl std::error::Error for ServiceProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidMessage(err) => Some(err),
            Self::Other(err) => Some(err),
            _ => None,
        }
    }
}

impl From<ServiceProtocolError> for Error {
    fn from(err: ServiceProtocolError) -> Self {
        if let ServiceProtocolError::Other(err) = err {
            err
        } else {
            Error::from_static_msg_and_cause("service connection error", err)
        }
    }
}
