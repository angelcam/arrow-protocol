use bytes::{Buf, BufMut};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, SizeError, Unaligned,
    byteorder::network_endian::{U16, U32},
};

use crate::v2::{
    msg::control::DecodingError,
    utils::{Decode, Encode, UnexpectedEof},
};

/// Status message payload.
#[derive(Copy, Clone)]
pub struct StatusMessage {
    request_id: u16,
    status_flags: u32,
    active_sessions: u32,
}

impl StatusMessage {
    pub const FLAG_SCANNING_NETWORK: u32 = 1;

    /// Create a new status message payload.
    #[inline]
    pub fn new(request_id: u16, status_flags: u32, active_sessions: u32) -> Self {
        Self {
            request_id,
            status_flags,
            active_sessions,
        }
    }

    /// Get the message ID of the corresponding request.
    #[inline]
    pub fn request_id(&self) -> u16 {
        self.request_id
    }

    /// Get status flags.
    #[inline]
    pub fn status_flags(&self) -> u32 {
        self.status_flags
    }

    /// Get number of active sessions.
    #[inline]
    pub fn active_sessions(&self) -> u32 {
        self.active_sessions
    }
}

impl Decode for StatusMessage {
    type Error = DecodingError;

    fn decode<B>(data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        let (raw, _) = RawMessage::ref_from_prefix(data.as_ref())
            .map_err(SizeError::from)
            .map_err(|_| UnexpectedEof)?;

        let res = Self {
            request_id: raw.request_id.get(),
            status_flags: raw.status_flags.get(),
            active_sessions: raw.active_sessions.get(),
        };

        data.advance(std::mem::size_of::<RawMessage>());

        Ok(res)
    }
}

impl Encode for StatusMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        let raw = RawMessage {
            request_id: U16::new(self.request_id),
            status_flags: U32::new(self.status_flags),
            active_sessions: U32::new(self.active_sessions),
        };

        buf.put_slice(raw.as_bytes())
    }

    #[inline]
    fn size(&self) -> usize {
        std::mem::size_of::<RawMessage>()
    }
}

/// Status message header.
#[derive(Copy, Clone, KnownLayout, Immutable, Unaligned, IntoBytes, FromBytes)]
#[repr(C)]
struct RawMessage {
    request_id: U16,
    status_flags: U32,
    active_sessions: U32,
}
