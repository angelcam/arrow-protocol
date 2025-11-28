use bytes::{Bytes, BytesMut};

use crate::v3::{
    error::Error,
    msg::{DecodeMessage, EncodeMessage, Message, MessageKind},
};

/// Redirect message.
pub struct RedirectMessage {
    target: String,
}

impl RedirectMessage {
    /// Get the redirect target.
    pub fn into_target(self) -> String {
        self.target
    }
}

impl Message for RedirectMessage {
    fn kind(&self) -> MessageKind {
        MessageKind::Redirect
    }
}

impl DecodeMessage for RedirectMessage {
    fn decode(buf: &mut Bytes) -> Result<Self, Error> {
        let null_pos = buf
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| Error::from_static_msg("invalid redirect message"))?;

        let target = str::from_utf8(&buf.split_to(null_pos))
            .map_err(|_| Error::from_static_msg("redirect target is not UTF-8 encoded"))?
            .to_string();

        let res = Self { target };

        Ok(res)
    }
}

impl EncodeMessage for RedirectMessage {
    fn encode(&self, buf: &mut BytesMut) -> Bytes {
        buf.reserve(1 + self.target.len());

        buf.extend_from_slice(self.target.as_bytes());
        buf.extend_from_slice(&[0]);

        let data = buf.split();

        data.freeze()
    }
}
