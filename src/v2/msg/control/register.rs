use bytes::{Buf, BufMut};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, SizeError, Unaligned,
    byteorder::network_endian::{U16, U32},
};

use crate::{
    ClientId, ClientKey, MacAddr,
    v2::{
        msg::control::{DecodingError, svc_table::ServiceTable},
        utils::{Decode, DecodeWithContext, Encode, UnexpectedEof},
    },
};

/// Register message payload.
#[derive(Clone)]
pub struct RegisterMessage {
    version: MessageVersion,
    client_id: ClientId,
    client_key: ClientKey,
    mac_address: MacAddr,
    window_size: u16,
    flags: u32,
    service_table: ServiceTable,
    extended_info: String,
}

impl RegisterMessage {
    /// Flag indicating that the client can be used as a gateway.
    pub const FLAG_GATEWAY_MODE: u32 = 0x01;

    /// Create a new register message payload.
    #[inline]
    pub fn new(
        client_id: ClientId,
        mac_address: MacAddr,
        client_key: ClientKey,
        service_table: ServiceTable,
    ) -> Self {
        Self {
            version: MessageVersion::V1,
            client_id,
            client_key,
            mac_address,
            window_size: 0,
            flags: 0,
            service_table,
            extended_info: String::new(),
        }
    }

    /// Create a new register message payload.
    #[inline]
    pub fn new_v2(
        client_id: ClientId,
        mac_address: MacAddr,
        client_key: ClientKey,
        service_table: ServiceTable,
    ) -> Self {
        Self {
            version: MessageVersion::V2,
            client_id,
            client_key,
            mac_address,
            window_size: u16::MAX,
            flags: 0,
            service_table,
            extended_info: String::new(),
        }
    }

    /// Get the client ID.
    #[inline]
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// Get the client secret key.
    #[inline]
    pub fn client_key(&self) -> ClientKey {
        self.client_key
    }

    /// Get the client MAC address.
    #[inline]
    pub fn mac_address(&self) -> MacAddr {
        self.mac_address
    }

    /// Get session window size.
    #[inline]
    pub fn window_size(&self) -> Option<u16> {
        if self.version == MessageVersion::V1 {
            None
        } else {
            Some(self.window_size)
        }
    }

    /// Set the window size.
    #[inline]
    pub fn with_window_size(mut self, window_size: u16) -> Self {
        self.window_size = window_size;
        self
    }

    /// Get the flags.
    #[inline]
    pub fn flags(&self) -> u32 {
        self.flags
    }

    /// Set the flags.
    #[inline]
    pub fn with_flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    /// Get the client service table.
    #[inline]
    pub fn service_table(&self) -> &ServiceTable {
        &self.service_table
    }

    /// Get extended info.
    #[inline]
    pub fn extended_info(&self) -> Option<&str> {
        if self.version == MessageVersion::V1 || self.extended_info.is_empty() {
            None
        } else {
            Some(&self.extended_info)
        }
    }

    /// Set the extended info.
    pub fn with_extended_info<T>(mut self, info: T) -> Self
    where
        T: ToString,
    {
        self.extended_info = info.to_string();
        self
    }

    /// Decode the first version of the REGISTER message.
    fn decode_v1<B>(data: &mut B) -> Result<Self, DecodingError>
    where
        B: Buf + AsRef<[u8]>,
    {
        let (header, _) = RawMessageHeader::ref_from_prefix(data.as_ref())
            .map_err(SizeError::from)
            .map_err(|_| UnexpectedEof)?;

        let client_id = ClientId::from_bytes(header.uuid);
        let mac_address = header.mac_address.into();
        let client_key = header.key;

        data.advance(std::mem::size_of::<RawMessageHeader>());

        let res = Self {
            version: MessageVersion::V1,
            client_id,
            client_key,
            mac_address,
            window_size: 0,
            flags: 0,
            service_table: ServiceTable::decode(data)?,
            extended_info: String::new(),
        };

        Ok(res)
    }

    /// Decode the second version of the REGISTER message.
    fn decode_v2<B>(data: &mut B) -> Result<Self, DecodingError>
    where
        B: Buf + AsRef<[u8]>,
    {
        let (header, _) = RawMessageHeaderV2::ref_from_prefix(data.as_ref())
            .map_err(SizeError::from)
            .map_err(|_| UnexpectedEof)?;

        let client_id = ClientId::from_bytes(header.uuid);
        let client_key = header.key;
        let mac_address = header.mac_address.into();
        let window_size = header.window_size.get();
        let flags = header.flags.get();

        data.advance(std::mem::size_of::<RawMessageHeaderV2>());

        let pos = data
            .as_ref()
            .iter()
            .position(|b| *b == 0)
            .ok_or(DecodingError::UnexpectedEof)?;

        let extended_info = std::str::from_utf8(&data.as_ref()[..pos])
            .map_err(|_| DecodingError::invalid_data("invalid extended info"))?
            .to_string();

        data.advance(pos + 1);

        let res = Self {
            version: MessageVersion::V2,
            client_id,
            client_key,
            mac_address,
            window_size,
            flags,
            service_table: ServiceTable::decode(data)?,
            extended_info,
        };

        Ok(res)
    }

    /// Encode the first version of the REGISTER message.
    fn encode_v1<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        debug_assert_eq!(self.version, MessageVersion::V1);

        let header = RawMessageHeader {
            uuid: self.client_id.into_bytes(),
            mac_address: self.mac_address.into_array(),
            key: self.client_key,
        };

        buf.put_slice(header.as_bytes());

        self.service_table.encode(buf);
    }

    /// Encode the second version of the REGISTER message.
    fn encode_v2<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        debug_assert_eq!(self.version, MessageVersion::V2);

        let header = RawMessageHeaderV2 {
            uuid: self.client_id.into_bytes(),
            key: self.client_key,
            mac_address: self.mac_address.into_array(),
            window_size: U16::new(self.window_size),
            flags: U32::new(self.flags),
        };

        buf.put_slice(header.as_bytes());

        buf.put_slice(self.extended_info.as_bytes());
        buf.put_u8(0);

        self.service_table.encode(buf);
    }
}

impl DecodeWithContext for RegisterMessage {
    type Context = u8;
    type Error = DecodingError;

    fn decode<B>(version: u8, data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        match version {
            v if v < 2 => Self::decode_v1(data),
            2 => Self::decode_v2(data),
            _ => Err(DecodingError::UnsupportedProtocolVersion(version)),
        }
    }
}

impl Encode for RegisterMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        match self.version {
            MessageVersion::V1 => self.encode_v1(buf),
            MessageVersion::V2 => self.encode_v2(buf),
        }
    }

    #[inline]
    fn size(&self) -> usize {
        match self.version {
            MessageVersion::V1 => {
                std::mem::size_of::<RawMessageHeader>() + self.service_table.size()
            }
            MessageVersion::V2 => {
                std::mem::size_of::<RawMessageHeaderV2>()
                    + self.extended_info.len()
                    + 1
                    + self.service_table.size()
            }
        }
    }
}

/// REGISTER message version.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum MessageVersion {
    V1,
    V2,
}

/// Register message header.
#[derive(Copy, Clone, KnownLayout, Immutable, Unaligned, IntoBytes, FromBytes)]
#[repr(C)]
struct RawMessageHeader {
    uuid: [u8; 16],
    mac_address: [u8; 6],
    key: [u8; 16],
}

/// Register message header.
#[derive(Copy, Clone, KnownLayout, Immutable, Unaligned, IntoBytes, FromBytes)]
#[repr(C)]
struct RawMessageHeaderV2 {
    uuid: [u8; 16],
    key: [u8; 16],
    mac_address: [u8; 6],
    window_size: U16,
    flags: U32,
}
