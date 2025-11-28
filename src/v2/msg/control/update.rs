use crate::v2::{
    msg::control::{DecodingError, svc_table::ServiceTable},
    utils::{Decode, Encode},
};

use bytes::{Buf, BufMut};

/// Update message payload.
#[derive(Clone)]
pub struct UpdateMessage {
    service_table: ServiceTable,
}

impl UpdateMessage {
    /// Create a new update message.
    #[inline]
    pub fn new(service_table: ServiceTable) -> Self {
        Self { service_table }
    }

    /// Get the service table.
    #[inline]
    pub fn service_table(&self) -> &ServiceTable {
        &self.service_table
    }
}

impl Decode for UpdateMessage {
    type Error = DecodingError;

    fn decode<B>(data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        ServiceTable::decode(data).map(Self::new)
    }
}

impl Encode for UpdateMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        self.service_table.encode(buf);
    }

    #[inline]
    fn size(&self) -> usize {
        self.service_table.size()
    }
}
