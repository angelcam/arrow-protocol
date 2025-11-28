use bytes::{Buf, BufMut};

use crate::v2::{
    msg::control::DecodingError,
    utils::{AsBytes, Decode, Encode, FromBytes},
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
        let raw = RawMessage::from_bytes(data.as_ref())?;

        let res = Self {
            session_id: u32::from_be(raw.session_id) & 0x00ffffff,
            error_code: u32::from_be(raw.error_code),
        };

        data.advance(raw.size());

        Ok(res)
    }
}

impl Encode for HupMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        let raw = RawMessage {
            session_id: self.session_id.to_be(),
            error_code: self.error_code.to_be(),
        };

        buf.put_slice(raw.as_bytes());
    }

    #[inline]
    fn size(&self) -> usize {
        std::mem::size_of::<RawMessage>()
    }
}

/// Raw HUP message.
#[repr(C, packed)]
#[derive(Copy, Clone)]
struct RawMessage {
    session_id: u32,
    error_code: u32,
}

impl AsBytes for RawMessage {}
impl FromBytes for RawMessage {}
