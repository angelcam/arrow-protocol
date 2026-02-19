use bytes::{Buf, BufMut};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, SizeError, Unaligned,
    byteorder::network_endian::U32,
};

use crate::v2::{
    msg::control::DecodingError,
    utils::{Decode, Encode, UnexpectedEof},
};

/// HUP message payload.
#[derive(Copy, Clone)]
pub struct HupMessage {
    session_id: u32,
    error_code: u32,
}

impl HupMessage {
    pub const NO_ERROR: u32 = 0;
    pub const CONNECTION_ERROR: u32 = 0x03;
    pub const INTERNAL_SERVER_ERROR: u32 = 0xffffffff;

    /// Create a new HUP message payload.
    pub fn new(session_id: u32, error_code: u32) -> Self {
        assert!((session_id >> 24) == 0);

        Self {
            session_id,
            error_code,
        }
    }

    /// Get the session ID.
    #[inline]
    pub fn session_id(&self) -> u32 {
        self.session_id
    }

    /// Get the error code.
    #[inline]
    pub fn error_code(&self) -> u32 {
        self.error_code
    }
}

impl Decode for HupMessage {
    type Error = DecodingError;

    fn decode<B>(data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        let (raw, _) = RawMessage::ref_from_prefix(data.as_ref())
            .map_err(SizeError::from)
            .map_err(|_| UnexpectedEof)?;

        let res = Self {
            session_id: raw.session_id.get() & 0x00ffffff,
            error_code: raw.error_code.get(),
        };

        data.advance(std::mem::size_of::<RawMessage>());

        Ok(res)
    }
}

impl Encode for HupMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        let raw = RawMessage {
            session_id: U32::new(self.session_id),
            error_code: U32::new(self.error_code),
        };

        buf.put_slice(raw.as_bytes());
    }

    #[inline]
    fn size(&self) -> usize {
        std::mem::size_of::<RawMessage>()
    }
}

/// Raw HUP message.
#[derive(Copy, Clone, KnownLayout, Immutable, Unaligned, IntoBytes, FromBytes)]
#[repr(C)]
struct RawMessage {
    session_id: U32,
    error_code: U32,
}
