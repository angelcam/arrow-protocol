use bytes::{Buf, BufMut};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, SizeError, Unaligned,
    byteorder::network_endian::U32,
};

use crate::v2::{
    msg::control::DecodingError,
    utils::{Decode, Encode, UnexpectedEof},
};

/// DATA_ACK message payload.
#[derive(Copy, Clone)]
pub struct DataAckMessage {
    session_id: u32,
    length: u32,
}

impl DataAckMessage {
    /// Create a new DATA_ACK message payload.
    pub fn new(session_id: u32, length: u32) -> Self {
        assert!((session_id >> 24) == 0);

        Self { session_id, length }
    }

    /// Get the session ID.
    #[inline]
    pub fn session_id(&self) -> u32 {
        self.session_id
    }

    /// Get the ACK length.
    #[inline]
    pub fn length(&self) -> u32 {
        self.length
    }
}

impl Decode for DataAckMessage {
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
            length: raw.length.get(),
        };

        data.advance(std::mem::size_of::<RawMessage>());

        Ok(res)
    }
}

impl Encode for DataAckMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        let raw = RawMessage {
            session_id: U32::new(self.session_id),
            length: U32::new(self.length),
        };

        buf.put_slice(raw.as_bytes());
    }

    #[inline]
    fn size(&self) -> usize {
        std::mem::size_of::<RawMessage>()
    }
}

/// Raw DATA_ACK message.
#[derive(Copy, Clone, KnownLayout, Immutable, Unaligned, IntoBytes, FromBytes)]
#[repr(C)]
struct RawMessage {
    session_id: U32,
    length: U32,
}
