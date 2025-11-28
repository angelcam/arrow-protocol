use bytes::{Buf, BufMut};

use crate::v2::{
    msg::control::DecodingError,
    utils::{Decode, Encode},
};

/// Redirect message payload.
#[derive(Clone)]
pub struct RedirectMessage {
    address: String,
}

impl RedirectMessage {
    /// Create a new redirect message payload.
    pub fn new<T>(address: T) -> Self
    where
        T: ToString,
    {
        Self {
            address: address.to_string(),
        }
    }

    /// Get the redirect address.
    #[inline]
    pub fn address(&self) -> &str {
        &self.address
    }
}

impl Decode for RedirectMessage {
    type Error = DecodingError;

    fn decode<B>(data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        let len = data
            .as_ref()
            .iter()
            .position(|b| *b == 0)
            .ok_or(DecodingError::UnexpectedEof)?;

        let address = std::str::from_utf8(&data.as_ref()[..len])
            .map_err(|_| DecodingError::invalid_data("redirect address is not UTF-8 encoded"))?
            .to_string();

        data.advance(len + 1);

        Ok(Self::new(address))
    }
}

impl Encode for RedirectMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put_slice(self.address.as_bytes());
        buf.put_u8(0);
    }

    #[inline]
    fn size(&self) -> usize {
        self.address.len() + 1
    }
}
