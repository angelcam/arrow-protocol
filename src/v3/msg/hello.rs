//! Hello message definitions.

use bytes::{Buf, BytesMut};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, SizeError, Unaligned,
    byteorder::network_endian::U32,
};

use crate::{
    ClientId, ClientKey, MacAddr,
    v3::{
        PROTOCOL_VERSION,
        error::Error,
        msg::{DecodeMessage, EncodeMessage, EncodedMessage, MessageKind},
    },
};

/// Control protocol hello message.
pub struct ControlProtocolHelloMessage {
    protocol_version: u8,
    client_id: ClientId,
    client_key: ClientKey,
    client_mac: MacAddr,
    flags: u32,
    extended_info: String,
}

impl ControlProtocolHelloMessage {
    /// Flag indicating that the client can be used as a gateway.
    pub const FLAG_GATEWAY_MODE: u32 = 0x01;

    /// Create a new hello message.
    #[inline]
    pub const fn new(client_id: ClientId, client_key: ClientKey, client_mac: MacAddr) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            client_id,
            client_key,
            client_mac,
            flags: 0,
            extended_info: String::new(),
        }
    }

    /// Set flags.
    #[inline]
    pub fn with_flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    /// Set extended info.
    pub fn with_extended_info<T>(mut self, extended_info: T) -> Self
    where
        T: Into<String>,
    {
        self.extended_info = extended_info.into();
        self
    }

    /// Get the protocol version.
    #[inline]
    pub fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    /// Get the client ID.
    #[inline]
    pub fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    /// Get the client key.
    #[inline]
    pub fn client_key(&self) -> &ClientKey {
        &self.client_key
    }

    /// Get the client MAC address.
    #[inline]
    pub fn client_mac(&self) -> &MacAddr {
        &self.client_mac
    }

    /// Get the flags.
    #[inline]
    pub fn flags(&self) -> u32 {
        self.flags
    }

    /// Get the extended info.
    #[inline]
    pub fn extended_info(&self) -> &str {
        &self.extended_info
    }
}

impl DecodeMessage for ControlProtocolHelloMessage {
    fn decode(encoded: &EncodedMessage) -> Result<Self, Error> {
        assert_eq!(encoded.kind(), MessageKind::ControlProtocolHello);

        let data = encoded.data();

        let mut buf = data.clone();

        let (raw, _) = RawControlProtocolHelloMessage::ref_from_prefix(&buf)
            .map_err(SizeError::from)
            .map_err(|_| Error::from_static_msg("control protocol hello message too short"))?;

        let mut res = Self {
            protocol_version: encoded.protocol_version(),
            client_id: ClientId::from_bytes(raw.client_id),
            client_key: raw.client_key,
            client_mac: MacAddr::from(raw.client_mac),
            flags: raw.flags.get(),
            extended_info: String::new(),
        };

        buf.advance(std::mem::size_of::<RawControlProtocolHelloMessage>());

        let null_pos = buf
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| Error::from_static_msg("invalid control protocol hello message"))?;

        res.extended_info = str::from_utf8(&buf.split_to(null_pos))
            .map_err(|_| Error::from_static_msg("extended info is not UTF-8 encoded"))?
            .to_string();

        Ok(res)
    }
}

impl EncodeMessage for ControlProtocolHelloMessage {
    fn encode(&self, buf: &mut BytesMut) -> EncodedMessage {
        let len =
            std::mem::size_of::<RawControlProtocolHelloMessage>() + self.extended_info.len() + 1;

        buf.reserve(len);

        let msg = RawControlProtocolHelloMessage {
            client_id: self.client_id.into_bytes(),
            client_key: self.client_key,
            client_mac: self.client_mac.into_array(),
            flags: U32::new(self.flags),
        };

        buf.extend_from_slice(msg.as_bytes());
        buf.extend_from_slice(self.extended_info.as_bytes());
        buf.extend_from_slice(&[0]);

        let data = buf.split();

        EncodedMessage::new(MessageKind::ControlProtocolHello, data.freeze())
    }
}

/// Raw representation of the control protocol hello message.
#[derive(Copy, Clone, KnownLayout, Immutable, Unaligned, IntoBytes, FromBytes)]
#[repr(C)]
struct RawControlProtocolHelloMessage {
    client_id: [u8; 16],
    client_key: [u8; 16],
    client_mac: [u8; 6],
    flags: U32,
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
    #[inline]
    pub fn access_token(&self) -> &str {
        &self.access_token
    }
}

impl DecodeMessage for ServiceProtocolHelloMessage {
    fn decode(encoded: &EncodedMessage) -> Result<Self, Error> {
        assert_eq!(encoded.kind(), MessageKind::ServiceProtocolHello);

        let data = encoded.data();

        let mut buf = data.clone();

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
    fn encode(&self, buf: &mut BytesMut) -> EncodedMessage {
        buf.reserve(1 + self.access_token.len());

        buf.extend_from_slice(self.access_token.as_bytes());
        buf.extend_from_slice(&[0]);

        let data = buf.split();

        EncodedMessage::new(MessageKind::ServiceProtocolHello, data.freeze())
    }
}
