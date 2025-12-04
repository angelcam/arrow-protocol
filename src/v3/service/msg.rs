use crate::v3::{
    msg::{
        DecodeMessage, EncodedMessage, MessageKind,
        error::ErrorMessage,
        raw::{RawDataAckMessage, RawDataMessage},
    },
    service::error::ServiceProtocolError,
};

/// Service connection message.
pub enum ServiceConnectionMessage {
    Data(RawDataMessage),
    DataAck(RawDataAckMessage),
    Error(ErrorMessage),
}

impl ServiceConnectionMessage {
    /// Decode a service connection message.
    pub fn decode(msg: &EncodedMessage) -> Result<Self, ServiceProtocolError> {
        match msg.kind() {
            MessageKind::RawData => Self::decode_message::<RawDataMessage>(msg),
            MessageKind::RawDataAck => Self::decode_message::<RawDataAckMessage>(msg),
            MessageKind::Error => Self::decode_message::<ErrorMessage>(msg),
            _ => Err(ServiceProtocolError::UnexpectedMessageType(msg.kind())),
        }
    }

    /// Decode a specific message type.
    fn decode_message<T>(msg: &EncodedMessage) -> Result<Self, ServiceProtocolError>
    where
        T: DecodeMessage,
        Self: From<T>,
    {
        let msg = T::decode(msg).map_err(ServiceProtocolError::InvalidMessage)?;

        let res = Self::from(msg);

        Ok(res)
    }
}

impl From<RawDataMessage> for ServiceConnectionMessage {
    fn from(msg: RawDataMessage) -> Self {
        Self::Data(msg)
    }
}

impl From<RawDataAckMessage> for ServiceConnectionMessage {
    fn from(msg: RawDataAckMessage) -> Self {
        Self::DataAck(msg)
    }
}

impl From<ErrorMessage> for ServiceConnectionMessage {
    fn from(msg: ErrorMessage) -> Self {
        Self::Error(msg)
    }
}
