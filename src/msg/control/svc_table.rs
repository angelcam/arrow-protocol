//! Service table.

use std::{
    borrow::Borrow,
    error::Error,
    fmt::{self, Display, Formatter},
    net::{IpAddr, Ipv4Addr},
    ops::Deref,
};

use bytes::{Buf, BufMut};

use crate::{
    msg::control::DecodingError,
    utils::{net::IpAddrExt, AsBytes, Decode, Encode, FromBytes},
    MacAddr,
};

/// Unknown service type.
#[derive(Debug, Copy, Clone)]
pub struct UnknownServiceType(u16);

impl Display for UnknownServiceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "unknown service type: 0x{:04x}", self.0)
    }
}

impl Error for UnknownServiceType {}

impl From<UnknownServiceType> for DecodingError {
    fn from(err: UnknownServiceType) -> Self {
        Self::invalid_data(err.to_string())
    }
}

/// Service type.
#[repr(u16)]
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[allow(clippy::upper_case_acronyms)]
pub enum ServiceType {
    ControlProtocol = 0x00,
    RTSP = 0x01,
    LockedRTSP = 0x02,
    UnknownRTSP = 0x03,
    UnsupportedRTSP = 0x04,
    HTTP = 0x05,
    MJPEG = 0x06,
    LockedMJPEG = 0x07,
    TCP = 0xffff,
}

impl TryFrom<u16> for ServiceType {
    type Error = UnknownServiceType;

    fn try_from(n: u16) -> Result<Self, Self::Error> {
        let res = match n {
            0x00 => Self::ControlProtocol,
            0x01 => Self::RTSP,
            0x02 => Self::LockedRTSP,
            0x03 => Self::UnknownRTSP,
            0x04 => Self::UnsupportedRTSP,
            0x05 => Self::HTTP,
            0x06 => Self::MJPEG,
            0x07 => Self::LockedMJPEG,
            0xffff => Self::TCP,
            _ => return Err(UnknownServiceType(n)),
        };

        Ok(res)
    }
}

/// Service table record.
#[derive(Clone)]
pub struct ServiceRecord {
    service_id: u16,
    service_type: ServiceType,
    mac_address: MacAddr,
    ip_address: IpAddr,
    port: u16,
    path: String,
}

impl ServiceRecord {
    /// Create a new service table record.
    pub fn new<T>(
        service_id: u16,
        service_type: ServiceType,
        mac_address: MacAddr,
        ip_address: IpAddr,
        port: u16,
        path: T,
    ) -> Self
    where
        T: ToString,
    {
        Self {
            service_id,
            service_type,
            mac_address,
            ip_address,
            port,
            path: path.to_string(),
        }
    }

    /// Get the service ID.
    #[inline]
    pub fn service_id(&self) -> u16 {
        self.service_id
    }

    /// Get the service type.
    #[inline]
    pub fn service_type(&self) -> ServiceType {
        self.service_type
    }

    /// Get the MAC address of the host.
    #[inline]
    pub fn mac_address(&self) -> MacAddr {
        self.mac_address
    }

    /// Get the IP address of the host.
    #[inline]
    pub fn ip_address(&self) -> IpAddr {
        self.ip_address
    }

    /// Get the port of the service.
    #[inline]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Get the service endpoint.
    #[inline]
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl Decode for ServiceRecord {
    type Error = DecodingError;

    fn decode<B>(data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        let header = RawRecordHeader::from_bytes(data.as_ref())?;

        let header_len = header.size();

        let rest = &data.as_ref()[header_len..];

        let path_len = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or(DecodingError::UnexpectedEof)?;

        let path = std::str::from_utf8(&rest[..path_len])
            .map_err(|_| DecodingError::invalid_data("service path is not UTF-8 encoded"))?;

        let service_id = u16::from_be(header.service_id);
        let service_type = ServiceType::try_from(u16::from_be(header.service_type))?;

        let res = if service_type == ServiceType::ControlProtocol {
            Self {
                service_id: 0,
                service_type,
                mac_address: MacAddr::nil(),
                ip_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                port: 0,
                path: String::new(),
            }
        } else {
            Self {
                service_id,
                service_type,
                mac_address: header.mac_address.into(),
                ip_address: IpAddr::decode(header.ip_version, header.ip_address)?,
                port: u16::from_be(header.port),
                path: path.to_string(),
            }
        };

        data.advance(header_len + path_len + 1);

        Ok(res)
    }
}

impl Encode for ServiceRecord {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        let service_type = self.service_type as u16;

        let header = RawRecordHeader {
            service_id: self.service_id.to_be(),
            service_type: service_type.to_be(),
            mac_address: self.mac_address.into_array(),
            ip_version: self.ip_address.version(),
            ip_address: self.ip_address.encode(),
            port: self.port.to_be(),
        };

        buf.put_slice(header.as_bytes());
        buf.put_slice(self.path.as_bytes());
        buf.put_u8(0);
    }

    #[inline]
    fn size(&self) -> usize {
        std::mem::size_of::<RawRecordHeader>() + self.path.len() + 1
    }
}

/// Service record header.
#[repr(packed)]
#[derive(Copy, Clone)]
struct RawRecordHeader {
    service_id: u16,
    service_type: u16,
    mac_address: [u8; 6],
    ip_version: u8,
    ip_address: [u8; 16],
    port: u16,
}

impl AsBytes for RawRecordHeader {}
impl FromBytes for RawRecordHeader {}

/// Service table.
#[derive(Clone)]
pub struct ServiceTable {
    records: Vec<ServiceRecord>,
}

impl Decode for ServiceTable {
    type Error = DecodingError;

    fn decode<B>(data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        let mut records = Vec::new();

        loop {
            let record = ServiceRecord::decode(data)?;

            if record.service_type() == ServiceType::ControlProtocol {
                break;
            }

            records.push(record);
        }

        let res = Self { records };

        Ok(res)
    }
}

impl Encode for ServiceTable {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        for record in &self.records {
            record.encode(buf);
        }

        let terminator = ServiceRecord {
            service_id: 0,
            service_type: ServiceType::ControlProtocol,
            mac_address: MacAddr::nil(),
            ip_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 0,
            path: String::new(),
        };

        terminator.encode(buf);
    }

    fn size(&self) -> usize {
        let sum: usize = self.records.iter().map(|r| r.size()).sum();

        sum + std::mem::size_of::<RawRecordHeader>() + 1
    }
}

impl Borrow<[ServiceRecord]> for ServiceTable {
    #[inline]
    fn borrow(&self) -> &[ServiceRecord] {
        &self.records
    }
}

impl AsRef<[ServiceRecord]> for ServiceTable {
    #[inline]
    fn as_ref(&self) -> &[ServiceRecord] {
        &self.records
    }
}

impl Deref for ServiceTable {
    type Target = [ServiceRecord];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.records
    }
}

impl<T> From<T> for ServiceTable
where
    T: Into<Vec<ServiceRecord>>,
{
    fn from(records: T) -> Self {
        Self {
            records: records.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ServiceRecord;

    use crate::utils::Decode;

    #[test]
    fn test_null_record_decoding() {
        let mut record: &[u8] = b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

        ServiceRecord::decode(&mut record).unwrap();
    }
}
