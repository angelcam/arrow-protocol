//! Redirect message definitions.

use bytes::BytesMut;

use crate::v3::{
    error::Error,
    msg::{DecodeMessage, EncodeMessage, EncodedMessage, MessageKind},
};

/// Redirect message.
pub struct RedirectMessage {
    target: String,
}

impl RedirectMessage {
    /// Create a new redirect message.
    pub fn new<T>(target: T) -> Self
    where
        T: Into<String>,
    {
        Self {
            target: target.into(),
        }
    }

    /// Get the redirect target.
    #[inline]
    pub fn into_target(self) -> String {
        self.target
    }
}

impl DecodeMessage for RedirectMessage {
    fn decode(encoded: &EncodedMessage) -> Result<Self, Error> {
        assert_eq!(encoded.kind(), MessageKind::Redirect);

        let data = encoded.data();

        let mut buf = data.clone();

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
    fn encode(&self, buf: &mut BytesMut) -> EncodedMessage {
        buf.reserve(1 + self.target.len());

        buf.extend_from_slice(self.target.as_bytes());
        buf.extend_from_slice(&[0]);

        let data = buf.split();

        EncodedMessage::new(MessageKind::Redirect, data.freeze())
    }
}
