use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

///
#[derive(Debug, Copy, Clone)]
pub struct UnsupportedIpAddrVersion;

impl Display for UnsupportedIpAddrVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("unsupported IP address version")
    }
}

impl Error for UnsupportedIpAddrVersion {}

/// Simple extensions for `IpAddr`.
pub trait IpAddrExt {
    /// Decode an IP address.
    fn decode(version: u8, data: [u8; 16]) -> Result<Self, UnsupportedIpAddrVersion>
    where
        Self: Sized;

    /// Encode the IP address.
    fn encode(&self) -> [u8; 16];

    /// Get version of the IP address.
    fn version(&self) -> u8;
}

impl IpAddrExt for IpAddr {
    fn decode(version: u8, data: [u8; 16]) -> Result<Self, UnsupportedIpAddrVersion> {
        let res = match version {
            4 => Self::V4(Ipv4Addr::decode(data)),
            6 => Self::V6(Ipv6Addr::decode(data)),
            _ => return Err(UnsupportedIpAddrVersion),
        };

        Ok(res)
    }

    fn encode(&self) -> [u8; 16] {
        match self {
            Self::V4(addr) => addr.encode(),
            Self::V6(addr) => addr.encode(),
        }
    }

    fn version(&self) -> u8 {
        match self {
            Self::V4(_) => 4,
            Self::V6(_) => 6,
        }
    }
}

/// Simple extensions for `Ipv4Addr` and `Ipv6Addr`.
trait IpvXAddrExt {
    /// Decode the address.
    fn decode(data: [u8; 16]) -> Self
    where
        Self: Sized;

    /// Encode the address.
    fn encode(&self) -> [u8; 16];
}

impl IpvXAddrExt for Ipv4Addr {
    fn decode(data: [u8; 16]) -> Self {
        // this is safe because the ip_address field is 16 bytes long and we
        // only want the first four
        let octets = unsafe { &*(data.as_ptr() as *const _ as *const [u8; 4]) };

        Ipv4Addr::from(*octets)
    }

    fn encode(&self) -> [u8; 16] {
        let mut res = [0u8; 16];

        // this is safe because we are going to access only the first four
        // bytes of the 16 byte long array
        let dst = unsafe { &mut *(&mut res as *mut _ as *mut [u8; 4]) };

        *dst = self.octets();

        res
    }
}

impl IpvXAddrExt for Ipv6Addr {
    fn decode(data: [u8; 16]) -> Self {
        Self::from(data)
    }

    fn encode(&self) -> [u8; 16] {
        self.octets()
    }
}
