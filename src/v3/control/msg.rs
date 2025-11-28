use bytes::Bytes;

use crate::v3::{
    control::error::ControlProtocolError,
    msg::{
        DecodeMessage, EncodedMessage, MessageKind,
        error::ErrorMessage,
        json::{JsonRpcNotification, JsonRpcRequest, JsonRpcResponse},
        redirect::RedirectMessage,
    },
};

/// Control protocol message.
pub enum ControlProtocolMessage {
    JsonRpcRequest(JsonRpcRequest),
    JsonRpcResponse(JsonRpcResponse),
    JsonRpcNotification(JsonRpcNotification),
    Redirect(RedirectMessage),
    Error(ErrorMessage),
}

impl ControlProtocolMessage {
    /// Decode a control protocol message.
    pub fn decode(msg: &EncodedMessage) -> Result<Self, ControlProtocolError> {
        let data = msg.data();

        match msg.kind() {
            MessageKind::JsonRpcRequest => {
                Self::decode_message::<JsonRpcRequest>(&mut data.clone())
            }
            MessageKind::JsonRpcResponse => {
                Self::decode_message::<JsonRpcResponse>(&mut data.clone())
            }
            MessageKind::JsonRpcNotification => {
                Self::decode_message::<JsonRpcNotification>(&mut data.clone())
            }
            MessageKind::Error => Self::decode_message::<ErrorMessage>(&mut data.clone()),
            MessageKind::Redirect => Self::decode_message::<RedirectMessage>(&mut data.clone()),
            _ => Err(ControlProtocolError::UnexpectedMessageType(msg.kind())),
        }
    }

    /// Decode a specific message type.
    fn decode_message<T>(data: &mut Bytes) -> Result<Self, ControlProtocolError>
    where
        T: DecodeMessage,
        Self: From<T>,
    {
        let msg = T::decode(data).map_err(ControlProtocolError::InvalidMessage)?;

        let res = Self::from(msg);

        Ok(res)
    }
}

impl From<JsonRpcRequest> for ControlProtocolMessage {
    fn from(msg: JsonRpcRequest) -> Self {
        Self::JsonRpcRequest(msg)
    }
}

impl From<JsonRpcResponse> for ControlProtocolMessage {
    fn from(msg: JsonRpcResponse) -> Self {
        Self::JsonRpcResponse(msg)
    }
}

impl From<JsonRpcNotification> for ControlProtocolMessage {
    fn from(msg: JsonRpcNotification) -> Self {
        Self::JsonRpcNotification(msg)
    }
}

impl From<ErrorMessage> for ControlProtocolMessage {
    fn from(msg: ErrorMessage) -> Self {
        Self::Error(msg)
    }
}

impl From<RedirectMessage> for ControlProtocolMessage {
    fn from(msg: RedirectMessage) -> Self {
        Self::Redirect(msg)
    }
}
