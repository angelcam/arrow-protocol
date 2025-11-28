use bytes::{Buf, Bytes, BytesMut};

use crate::v3::{
    error::Error,
    msg::{DecodeMessage, EncodeMessage, Message, MessageKind},
    utils::{AsBytes, FromBytes},
};

/// Control protocol connection options.
pub struct ControlConnectionOptions {
    max_payload_size: u32,
    max_concurrent_requests: u16,
}

impl ControlConnectionOptions {
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

impl Message for ControlConnectionOptions {
    fn kind(&self) -> MessageKind {
        MessageKind::ControlConnectionOptions
    }
}

impl DecodeMessage for ControlConnectionOptions {
    fn decode(buf: &mut Bytes) -> Result<Self, Error> {
        let size = std::mem::size_of::<RawControlConnectionOptions>();

        if buf.len() < size {
            return Err(Error::from_static_msg(
                "control connection options message too short",
            ));
        }

        let raw = RawControlConnectionOptions::from_bytes(buf);

        let res = Self {
            max_payload_size: u32::from_be(raw.max_payload_size),
            max_concurrent_requests: u16::from_be(raw.max_concurrent_requests),
        };

        buf.advance(size);

        Ok(res)
    }
}

impl EncodeMessage for ControlConnectionOptions {
    fn encode(&self, buf: &mut BytesMut) -> Bytes {
        let msg = RawControlConnectionOptions {
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
struct RawControlConnectionOptions {
    max_payload_size: u32,
    max_concurrent_requests: u16,
}

/// Service connection options.
pub struct ServiceConnectionOptions {
    max_payload_size: u32,
    max_unacknowledged_data: u32,
}

impl ServiceConnectionOptions {
    /// Create new service connection options.
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

impl Message for ServiceConnectionOptions {
    fn kind(&self) -> MessageKind {
        MessageKind::ServiceConnectionOptions
    }
}

impl DecodeMessage for ServiceConnectionOptions {
    fn decode(buf: &mut Bytes) -> Result<Self, Error> {
        let size = std::mem::size_of::<RawServiceConnectionOptions>();

        if buf.len() < size {
            return Err(Error::from_static_msg(
                "service connection options message too short",
            ));
        }

        let raw = RawServiceConnectionOptions::from_bytes(buf);

        let res = Self {
            max_payload_size: u32::from_be(raw.max_payload_size),
            max_unacknowledged_data: u32::from_be(raw.max_unacknowledged_data),
        };

        buf.advance(size);

        Ok(res)
    }
}

impl EncodeMessage for ServiceConnectionOptions {
    fn encode(&self, buf: &mut BytesMut) -> Bytes {
        let msg = RawServiceConnectionOptions {
            max_payload_size: self.max_payload_size.to_be(),
            max_unacknowledged_data: self.max_unacknowledged_data.to_be(),
        };

        buf.extend_from_slice(msg.as_bytes());

        let data = buf.split();

        data.freeze()
    }
}

/// Raw representation of service connection options.
#[repr(packed, C)]
#[derive(Copy, Clone)]
struct RawServiceConnectionOptions {
    max_payload_size: u32,
    max_unacknowledged_data: u32,
}
