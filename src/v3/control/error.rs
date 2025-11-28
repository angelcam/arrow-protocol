use std::fmt::{self, Display, Formatter};

use crate::v3::{
    error::Error,
    msg::{MessageKind, error::ErrorMessage},
};

/// Control protocol connection error.
#[derive(Debug)]
pub enum ControlProtocolConnectionError {
    UnsupportedProtocolVersion,
    Unauthorized,
    Other(Error),
}

impl Display for ControlProtocolConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedProtocolVersion => f.write_str("unsupported protocol version"),
            Self::Unauthorized => f.write_str("unauthorized"),
            Self::Other(err) => Display::fmt(err, f),
        }
    }
}

impl std::error::Error for ControlProtocolConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Self::Other(err) = self {
            Some(err)
        } else {
            None
        }
    }
}

impl From<ErrorMessage> for ControlProtocolConnectionError {
    fn from(msg: ErrorMessage) -> Self {
        match msg {
            ErrorMessage::UnsupportedProtocolVersion => Self::UnsupportedProtocolVersion,
            ErrorMessage::Unauthorized => Self::Unauthorized,
            _ => Self::Other(Error::from_msg(format!("received error message: {msg}"))),
        }
    }
}

impl From<ControlProtocolError> for ControlProtocolConnectionError {
    fn from(err: ControlProtocolError) -> Self {
        Self::Other(Error::from(err))
    }
}

/// Control protocol error.
#[derive(Debug)]
pub enum ControlProtocolError {
    InvalidMessage(Error),
    UnexpectedMessageType(MessageKind),
    TooManyConcurrentRequests,
    UnexpectedResponseId(u64),
    Other(Error),
}

impl ControlProtocolError {
    /// Get an error message to be sent over to the remote peer (if any).
    pub fn to_error_message(&self) -> Option<ErrorMessage> {
        let res = match self {
            Self::InvalidMessage(_) => ErrorMessage::InvalidMessage,
            Self::UnexpectedMessageType(_) => ErrorMessage::UnexpectedMessageType,
            Self::TooManyConcurrentRequests => ErrorMessage::TooManyConcurrentRequests,
            Self::UnexpectedResponseId(_) => ErrorMessage::UnexpectedResponseId,
            Self::Other(_) => return None,
        };

        Some(res)
    }
}

impl Display for ControlProtocolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMessage(err) => write!(f, "invalid message: {err}"),
            Self::UnexpectedMessageType(t) => {
                write!(f, "unexpected message type: {:02x}", *t as u8)
            }
            Self::TooManyConcurrentRequests => f.write_str("too many concurrent requests"),
            Self::UnexpectedResponseId(id) => write!(f, "unexpected response ID: {id:x}"),
            Self::Other(err) => Display::fmt(err, f),
        }
    }
}

impl std::error::Error for ControlProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidMessage(err) => Some(err),
            Self::Other(err) => Some(err),
            _ => None,
        }
    }
}

impl From<ControlProtocolError> for Error {
    fn from(err: ControlProtocolError) -> Self {
        if let ControlProtocolError::Other(err) = err {
            err
        } else {
            Error::from_static_msg_and_cause("control protocol error", err)
        }
    }
}
