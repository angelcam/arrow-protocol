use bytes::{Buf, Bytes, BytesMut};

use crate::v3::{
    error::Error,
    msg::{DecodeMessage, EncodeMessage, Message, MessageKind},
    utils::{AsBytes, FromBytes},
};

/// Control protocol connection options.
pub struct ControlProtocolOptions {
    max_payload_size: u32,
    max_concurrent_requests: u16,
}

impl ControlProtocolOptions {
    /// Create new control protocol connection options.
    pub const fn new(max_payload_size: u32, max_concurrent_requests: u16) -> Self {
        Self {
            max_payload_size,
            max_concurrent_requests,
        }
    }

    /// Get the maximum payload size.
    pub fn max_payload_size(&self) -> u32 {
        self.max_payload_size
    }

    /// Get the maximum number of concurrent requests.
    pub fn max_concurrent_requests(&self) -> u16 {
        self.max_concurrent_requests
    }
}

impl Message for ControlProtocolOptions {
    fn kind(&self) -> MessageKind {
        MessageKind::ControlProtocolOptions
    }
}

impl DecodeMessage for ControlProtocolOptions {
    fn decode(buf: &mut Bytes) -> Result<Self, Error> {
        let size = std::mem::size_of::<RawControlProtocolOptions>();

        if buf.len() < size {
            return Err(Error::from_static_msg(
                "control protocol options message too short",
            ));
        }

        let raw = RawControlProtocolOptions::from_bytes(buf);

        let res = Self {
            max_payload_size: u32::from_be(raw.max_payload_size),
            max_concurrent_requests: u16::from_be(raw.max_concurrent_requests),
        };

        buf.advance(size);

        Ok(res)
    }
}

impl EncodeMessage for ControlProtocolOptions {
    fn encode(&self, buf: &mut BytesMut) -> Bytes {
        let msg = RawControlProtocolOptions {
            max_payload_size: self.max_payload_size.to_be(),
            max_concurrent_requests: self.max_concurrent_requests.to_be(),
        };

        buf.extend_from_slice(msg.as_bytes());

        let data = buf.split();

        data.freeze()
    }
}

/// Raw representation of control protocol connection options.
#[repr(packed, C)]
#[derive(Copy, Clone)]
struct RawControlProtocolOptions {
    max_payload_size: u32,
    max_concurrent_requests: u16,
}

/// Service protocol connection options.
pub struct ServiceProtocolOptions {
    max_payload_size: u32,
    max_unacknowledged_data: u32,
}

impl ServiceProtocolOptions {
    /// Create new service protocol connection options.
    pub const fn new(max_payload_size: u32, max_unacknowledged_data: u32) -> Self {
        Self {
            max_payload_size,
            max_unacknowledged_data,
        }
    }

    /// Get the maximum payload size.
    pub fn max_payload_size(&self) -> u32 {
        self.max_payload_size
    }

    /// Get the maximum amount of unacknowledged data.
    pub fn max_unacknowledged_data(&self) -> u32 {
        self.max_unacknowledged_data
    }
}

impl Message for ServiceProtocolOptions {
    fn kind(&self) -> MessageKind {
        MessageKind::ServiceProtocolOptions
    }
}

impl DecodeMessage for ServiceProtocolOptions {
    fn decode(buf: &mut Bytes) -> Result<Self, Error> {
        let size = std::mem::size_of::<RawServiceProtocolOptions>();

        if buf.len() < size {
            return Err(Error::from_static_msg(
                "service protocol options message too short",
            ));
        }

        let raw = RawServiceProtocolOptions::from_bytes(buf);

        let res = Self {
            max_payload_size: u32::from_be(raw.max_payload_size),
            max_unacknowledged_data: u32::from_be(raw.max_unacknowledged_data),
        };

        buf.advance(size);

        Ok(res)
    }
}

impl EncodeMessage for ServiceProtocolOptions {
    fn encode(&self, buf: &mut BytesMut) -> Bytes {
        let msg = RawServiceProtocolOptions {
            max_payload_size: self.max_payload_size.to_be(),
            max_unacknowledged_data: self.max_unacknowledged_data.to_be(),
        };

        buf.extend_from_slice(msg.as_bytes());

        let data = buf.split();

        data.freeze()
    }
}

/// Raw representation of service protocol connection options.
#[repr(packed, C)]
#[derive(Copy, Clone)]
struct RawServiceProtocolOptions {
    max_payload_size: u32,
    max_unacknowledged_data: u32,
}
