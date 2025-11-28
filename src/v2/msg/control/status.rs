use bytes::{Buf, BufMut};

use crate::v2::{
    msg::control::DecodingError,
    utils::{AsBytes, Decode, Encode, FromBytes},
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
        let raw = RawMessage::from_bytes(data.as_ref())?;

        let res = Self {
            request_id: u16::from_be(raw.request_id),
            status_flags: u32::from_be(raw.status_flags),
            active_sessions: u32::from_be(raw.active_sessions),
        };

        data.advance(raw.size());

        Ok(res)
    }
}

impl Encode for StatusMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        let raw = RawMessage {
            request_id: self.request_id.to_be(),
            status_flags: self.status_flags.to_be(),
            active_sessions: self.active_sessions.to_be(),
        };

        buf.put_slice(raw.as_bytes())
    }

    #[inline]
    fn size(&self) -> usize {
        std::mem::size_of::<RawMessage>()
    }
}

/// Status message header.
#[repr(C, packed)]
#[derive(Copy, Clone)]
struct RawMessage {
    request_id: u16,
    status_flags: u32,
    active_sessions: u32,
}

impl AsBytes for RawMessage {}
impl FromBytes for RawMessage {}
