use bytes::{Buf, BufMut};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, SizeError, Unaligned,
    byteorder::network_endian::{U16, U32},
};

use crate::v2::{
    msg::control::DecodingError,
    utils::{Decode, Encode, UnexpectedEof},
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
        let (raw, _) = RawMessage::ref_from_prefix(data.as_ref())
            .map_err(SizeError::from)
            .map_err(|_| UnexpectedEof)?;

        let res = Self {
            service_id: raw.service_id.get(),
            session_id: raw.session_id.get() & 0x00ffffff,
        };

        data.advance(std::mem::size_of::<RawMessage>());

        Ok(res)
    }
}

impl Encode for ConnectMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        let raw = RawMessage {
            service_id: U16::new(self.service_id),
            session_id: U32::new(self.session_id),
        };

        buf.put_slice(raw.as_bytes());
    }

    #[inline]
    fn size(&self) -> usize {
        std::mem::size_of::<RawMessage>()
    }
}

/// Raw CONNECT message.
#[derive(Copy, Clone, KnownLayout, Immutable, Unaligned, IntoBytes, FromBytes)]
#[repr(C)]
struct RawMessage {
    service_id: U16,
    session_id: U32,
}
