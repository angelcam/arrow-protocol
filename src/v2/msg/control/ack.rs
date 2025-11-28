use bytes::{Buf, BufMut};

use crate::v2::{
    msg::control::DecodingError,
    utils::{Decode, Encode},
};

/// ACK message payload.
#[derive(Copy, Clone)]
pub struct AckMessage {
    code: u32,
}

impl AckMessage {
    pub const OK: u32 = 0x00;
    pub const UNSUPPORTED_PROTOCOL_VERSION: u32 = 0x01;
    pub const UNAUTHORIZED: u32 = 0x02;
    pub const CONNECTION_ERROR: u32 = 0x03;
    pub const UNSUPPORTED_METHOD: u32 = 0x04;
    pub const INTERNAL_SERVER_ERROR: u32 = 0xffffffff;

    /// Create a new ACK message payload.
    #[inline]
    pub fn new(code: u32) -> Self {
        Self { code }
    }

    /// Get the ACK code.
    #[inline]
    pub fn code(&self) -> u32 {
        self.code
    }
}

impl Decode for AckMessage {
    type Error = DecodingError;

    fn decode<B>(data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        if data.remaining() < std::mem::size_of::<u32>() {
            return Err(DecodingError::UnexpectedEof);
        }

        Ok(Self::new(data.get_u32()))
    }
}

impl Encode for AckMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put_u32(self.code);
    }

    #[inline]
    fn size(&self) -> usize {
        std::mem::size_of::<u32>()
    }
}
