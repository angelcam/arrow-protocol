//! Options message definitions.

use bytes::BytesMut;
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, SizeError, Unaligned,
    byteorder::network_endian::{U16, U32},
};

use crate::v3::{
    error::Error,
    msg::{DecodeMessage, EncodeMessage, EncodedMessage, MessageKind},
};

/// Control protocol connection options.
pub struct ControlProtocolOptions {
    max_payload_size: u32,
    max_concurrent_requests: u16,
}

impl ControlProtocolOptions {
    /// Create new control protocol connection options.
    #[inline]
    pub const fn new(max_payload_size: u32, max_concurrent_requests: u16) -> Self {
        Self {
            max_payload_size,
            max_concurrent_requests,
        }
    }

    /// Get the maximum payload size.
    #[inline]
    pub fn max_payload_size(&self) -> u32 {
        self.max_payload_size
    }

    /// Get the maximum number of concurrent requests.
    #[inline]
    pub fn max_concurrent_requests(&self) -> u16 {
        self.max_concurrent_requests
    }
}

impl DecodeMessage for ControlProtocolOptions {
    fn decode(encoded: &EncodedMessage) -> Result<Self, Error> {
        assert_eq!(encoded.kind(), MessageKind::ControlProtocolOptions);

        let (raw, _) = RawControlProtocolOptions::ref_from_prefix(encoded.data())
            .map_err(SizeError::from)
            .map_err(|_| Error::from_static_msg("control protocol options message too short"))?;

        let res = Self {
            max_payload_size: raw.max_payload_size.get(),
            max_concurrent_requests: raw.max_concurrent_requests.get(),
        };

        Ok(res)
    }
}

impl EncodeMessage for ControlProtocolOptions {
    fn encode(&self, buf: &mut BytesMut) -> EncodedMessage {
        let msg = RawControlProtocolOptions {
            max_payload_size: U32::new(self.max_payload_size),
            max_concurrent_requests: U16::new(self.max_concurrent_requests),
        };

        buf.extend_from_slice(msg.as_bytes());

        let data = buf.split();

        EncodedMessage::new(MessageKind::ControlProtocolOptions, data.freeze())
    }
}

/// Raw representation of control protocol connection options.
#[derive(Copy, Clone, KnownLayout, Immutable, Unaligned, IntoBytes, FromBytes)]
#[repr(C)]
struct RawControlProtocolOptions {
    max_payload_size: U32,
    max_concurrent_requests: U16,
}

/// Service protocol connection options.
pub struct ServiceProtocolOptions {
    max_payload_size: u32,
    max_unacknowledged_data: u32,
}

impl ServiceProtocolOptions {
    /// Create new service protocol connection options.
    #[inline]
    pub const fn new(max_payload_size: u32, max_unacknowledged_data: u32) -> Self {
        Self {
            max_payload_size,
            max_unacknowledged_data,
        }
    }

    /// Get the maximum payload size.
    #[inline]
    pub fn max_payload_size(&self) -> u32 {
        self.max_payload_size
    }

    /// Get the maximum amount of unacknowledged data.
    #[inline]
    pub fn max_unacknowledged_data(&self) -> u32 {
        self.max_unacknowledged_data
    }
}

impl DecodeMessage for ServiceProtocolOptions {
    fn decode(encoded: &EncodedMessage) -> Result<Self, Error> {
        assert_eq!(encoded.kind(), MessageKind::ServiceProtocolOptions);

        let (raw, _) = RawServiceProtocolOptions::ref_from_prefix(encoded.data())
            .map_err(SizeError::from)
            .map_err(|_| Error::from_static_msg("service protocol options message too short"))?;

        let res = Self {
            max_payload_size: raw.max_payload_size.get(),
            max_unacknowledged_data: raw.max_unacknowledged_data.get(),
        };

        Ok(res)
    }
}

impl EncodeMessage for ServiceProtocolOptions {
    fn encode(&self, buf: &mut BytesMut) -> EncodedMessage {
        let msg = RawServiceProtocolOptions {
            max_payload_size: U32::new(self.max_payload_size),
            max_unacknowledged_data: U32::new(self.max_unacknowledged_data),
        };

        buf.extend_from_slice(msg.as_bytes());

        let data = buf.split();

        EncodedMessage::new(MessageKind::ServiceProtocolOptions, data.freeze())
    }
}

/// Raw representation of service protocol connection options.
#[derive(Copy, Clone, KnownLayout, Immutable, Unaligned, IntoBytes, FromBytes)]
#[repr(C)]
struct RawServiceProtocolOptions {
    max_payload_size: U32,
    max_unacknowledged_data: U32,
}
