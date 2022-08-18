use bytes::{Buf, BufMut};

use crate::{
    msg::control::DecodingError,
    utils::{AsBytes, Decode, Encode, FromBytes},
};

/// CONNECT message payload.
#[derive(Copy, Clone)]
pub struct ConnectMessage {
    service_id: u16,
    session_id: u32,
}

impl ConnectMessage {
    /// Create a new CONNECT message payload.
    #[inline]
    pub const fn new(service_id: u16, session_id: u32) -> Self {
        assert!(service_id != 0);
        assert!((session_id >> 24) == 0);

        Self {
            service_id,
            session_id,
        }
    }

    /// Get the service ID.
    #[inline]
    pub fn service_id(&self) -> u16 {
        self.service_id
    }

    /// Get the session ID.
    #[inline]
    pub fn session_id(&self) -> u32 {
        self.session_id
    }
}

impl Decode for ConnectMessage {
    type Error = DecodingError;

    fn decode<B>(data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        let raw = RawMessage::from_bytes(data.as_ref())?;

        let res = Self {
            service_id: u16::from_be(raw.service_id),
            session_id: u32::from_be(raw.session_id) & 0x00ffffff,
        };

        data.advance(raw.size());

        Ok(res)
    }
}

impl Encode for ConnectMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        let raw = RawMessage {
            service_id: self.service_id.to_be(),
            session_id: self.session_id.to_be(),
        };

        buf.put_slice(raw.as_bytes());
    }

    #[inline]
    fn size(&self) -> usize {
        std::mem::size_of::<RawMessage>()
    }
}

/// Raw CONNECT message.
#[repr(packed)]
#[derive(Copy, Clone)]
struct RawMessage {
    service_id: u16,
    session_id: u32,
}

impl AsBytes for RawMessage {}
impl FromBytes for RawMessage {}
