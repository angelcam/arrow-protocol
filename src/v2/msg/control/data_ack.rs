use bytes::{Buf, BufMut};

use crate::v2::{
    msg::control::DecodingError,
    utils::{AsBytes, Decode, Encode, FromBytes},
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
        let raw = RawMessage::from_bytes(data.as_ref())?;

        let res = Self {
            session_id: u32::from_be(raw.session_id) & 0x00ffffff,
            length: u32::from_be(raw.length),
        };

        data.advance(raw.size());

        Ok(res)
    }
}

impl Encode for DataAckMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        let raw = RawMessage {
            session_id: self.session_id.to_be(),
            length: self.length.to_be(),
        };

        buf.put_slice(raw.as_bytes());
    }

    #[inline]
    fn size(&self) -> usize {
        std::mem::size_of::<RawMessage>()
    }
}

/// Raw DATA_ACK message.
#[repr(C, packed)]
#[derive(Copy, Clone)]
struct RawMessage {
    session_id: u32,
    length: u32,
}

impl AsBytes for RawMessage {}
impl FromBytes for RawMessage {}
