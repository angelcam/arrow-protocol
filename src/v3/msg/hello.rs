use bytes::{Buf, Bytes, BytesMut};

use crate::{
    ClientId, ClientKey, MacAddr,
    v3::{
        error::Error,
        msg::{DecodeMessage, EncodeMessage, Message, MessageKind},
        utils::{AsBytes, FromBytes},
    },
};

/// Control protocol hello message.
pub struct ControlProtocolHelloMessage {
    client_id: ClientId,
    client_key: ClientKey,
    client_mac: MacAddr,
}

impl ControlProtocolHelloMessage {
    /// Create a new hello message.
    pub const fn new(client_id: ClientId, client_key: ClientKey, client_mac: MacAddr) -> Self {
        Self {
            client_id,
            client_key,
            client_mac,
        }
    }

    /// Get the client ID.
    pub fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    /// Get the client key.
    pub fn client_key(&self) -> &ClientKey {
        &self.client_key
    }

    /// Get the client MAC address.
    pub fn client_mac(&self) -> &MacAddr {
        &self.client_mac
    }
}

impl Message for ControlProtocolHelloMessage {
    fn kind(&self) -> MessageKind {
        MessageKind::ControlProtocolHello
    }
}

impl DecodeMessage for ControlProtocolHelloMessage {
    fn decode(buf: &mut Bytes) -> Result<Self, Error> {
        let size = std::mem::size_of::<RawControlProtocolHelloMessage>();

        if buf.len() < size {
            return Err(Error::from_static_msg(
                "control protocol hello message too short",
            ));
        }

        let raw = RawControlProtocolHelloMessage::from_bytes(buf);

        let res = Self {
            client_id: ClientId::from_bytes(raw.client_id),
            client_key: raw.client_key,
            client_mac: MacAddr::from(raw.client_mac),
        };

        buf.advance(size);

        Ok(res)
    }
}

impl EncodeMessage for ControlProtocolHelloMessage {
    fn encode(&self, buf: &mut BytesMut) -> Bytes {
        let msg = RawControlProtocolHelloMessage {
            client_id: self.client_id.into_bytes(),
            client_key: self.client_key,
            client_mac: self.client_mac.into_array(),
        };

        buf.extend_from_slice(msg.as_bytes());

        let data = buf.split();

        data.freeze()
    }
}

/// Raw representation of the control protocol hello message.
#[repr(packed, C)]
#[derive(Copy, Clone)]
struct RawControlProtocolHelloMessage {
    client_id: [u8; 16],
    client_key: [u8; 16],
    client_mac: [u8; 6],
}

/// Service protocol hello message.
pub struct ServiceProtocolHelloMessage {
    access_token: String,
}

impl ServiceProtocolHelloMessage {
    /// Create a new hello message.
    pub fn new<T>(access_token: T) -> Self
    where
        T: Into<String>,
    {
        Self {
            access_token: access_token.into(),
        }
    }

    /// Get the access token.
    pub fn access_token(&self) -> &str {
        &self.access_token
    }
}

impl Message for ServiceProtocolHelloMessage {
    fn kind(&self) -> MessageKind {
        MessageKind::ServiceProtocolHello
    }
}

impl DecodeMessage for ServiceProtocolHelloMessage {
    fn decode(buf: &mut Bytes) -> Result<Self, Error> {
        let null_pos = buf
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| Error::from_static_msg("invalid service protocol hello message"))?;

        let access_token = str::from_utf8(&buf.split_to(null_pos))
            .map_err(|_| Error::from_static_msg("access token is not UTF-8 encoded"))?
            .to_string();

        let res = Self { access_token };

        Ok(res)
    }
}

impl EncodeMessage for ServiceProtocolHelloMessage {
    fn encode(&self, buf: &mut BytesMut) -> Bytes {
        buf.reserve(1 + self.access_token.len());

        buf.extend_from_slice(self.access_token.as_bytes());
        buf.extend_from_slice(&[0]);

        let data = buf.split();

        data.freeze()
    }
}
