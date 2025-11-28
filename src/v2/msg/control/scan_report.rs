//! Scan report.

use std::net::IpAddr;

use bytes::{Buf, BufMut};

use crate::{
    MacAddr,
    v2::{
        msg::control::{DecodingError, svc_table::ServiceTable},
        utils::{AsBytes, Decode, Encode, FromBytes, net::IpAddrExt},
    },
};

/// Scan report message payload.
#[derive(Clone)]
pub struct ScanReportMessage {
    request_id: u16,
    hosts: Vec<HostRecord>,
    service_table: ServiceTable,
}

impl ScanReportMessage {
    /// Create a new scan report payload.
    pub fn new<T>(request_id: u16, hosts: T, service_table: ServiceTable) -> Self
    where
        T: Into<Vec<HostRecord>>,
    {
        Self {
            request_id,
            hosts: hosts.into(),
            service_table,
        }
    }

    /// Get the message ID of the corresponding request.
    #[inline]
    pub fn request_id(&self) -> u16 {
        self.request_id
    }

    /// Get a list of discovered hosts.
    #[inline]
    pub fn hosts(&self) -> &[HostRecord] {
        &self.hosts
    }

    /// Get the service table.
    #[inline]
    pub fn service_table(&self) -> &ServiceTable {
        &self.service_table
    }
}

impl Decode for ScanReportMessage {
    type Error = DecodingError;

    fn decode<B>(data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        let header = RawMessageHeader::from_bytes(data.as_ref())?;

        let request_id = u16::from_be(header.request_id);
        let host_count = u32::from_be(header.host_count) as usize;

        data.advance(header.size());

        let mut hosts = Vec::with_capacity(host_count);

        for _ in 0..host_count {
            hosts.push(HostRecord::decode(data)?);
        }

        let service_table = ServiceTable::decode(data)?;

        let res = Self {
            request_id,
            hosts,
            service_table,
        };

        Ok(res)
    }
}

impl Encode for ScanReportMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        let host_count = self.hosts.len() as u32;

        let header = RawMessageHeader {
            request_id: self.request_id.to_be(),
            host_count: host_count.to_be(),
        };

        buf.put_slice(header.as_bytes());

        for host in &self.hosts {
            host.encode(buf);
        }

        self.service_table.encode(buf);
    }

    fn size(&self) -> usize {
        let hosts_size: usize = self.hosts.iter().map(|host| host.size()).sum();

        std::mem::size_of::<RawMessageHeader>() + hosts_size + self.service_table.size()
    }
}

/// Scan report header.
#[repr(C, packed)]
#[derive(Copy, Clone)]
struct RawMessageHeader {
    request_id: u16,
    host_count: u32,
}

impl AsBytes for RawMessageHeader {}
impl FromBytes for RawMessageHeader {}

/// Scan report host.
#[derive(Clone)]
pub struct HostRecord {
    flags: u8,
    mac_address: MacAddr,
    ip_address: IpAddr,
    ports: Vec<u16>,
}

impl HostRecord {
    pub const FLAG_ARP_SCAN: u8 = 1;
    pub const FLAG_ICMP_SCAN: u8 = 2;

    /// Create a new scan report host record.
    pub fn new<T>(flags: u8, mac_address: MacAddr, ip_address: IpAddr, ports: T) -> Self
    where
        T: Into<Vec<u16>>,
    {
        let ports = ports.into();

        assert!(ports.len() <= (u16::MAX as usize));

        Self {
            flags,
            mac_address,
            ip_address,
            ports,
        }
    }

    /// Get the flags.
    #[inline]
    pub fn flags(&self) -> u8 {
        self.flags
    }

    /// Get the host MAC address.
    #[inline]
    pub fn mac_address(&self) -> MacAddr {
        self.mac_address
    }

    /// Get the host IP address.
    #[inline]
    pub fn ip_address(&self) -> IpAddr {
        self.ip_address
    }

    /// Get list of open ports at the host.
    #[inline]
    pub fn ports(&self) -> &[u16] {
        &self.ports
    }
}

impl Decode for HostRecord {
    type Error = DecodingError;

    fn decode<B>(data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        let header = RawHostRecordHeader::from_bytes(data.as_ref())?;

        let header_len = header.size();

        let ip_address = IpAddr::decode(header.ip_version, header.ip_address)?;

        let port_count = u16::from_be(header.port_count) as usize;

        let rest = &data.as_ref()[header_len..];

        if rest.len() < (port_count << 1) {
            return Err(DecodingError::UnexpectedEof);
        }

        let be_ports =
            unsafe { std::slice::from_raw_parts(rest as *const _ as *const u16, port_count) };

        let ports = be_ports.iter().copied().map(u16::from_be).collect();

        let res = Self {
            flags: header.flags,
            mac_address: header.mac_address.into(),
            ip_address,
            ports,
        };

        data.advance(header_len + (port_count << 1));

        Ok(res)
    }
}

impl Encode for HostRecord {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        let port_count = self.ports.len() as u16;

        let header = RawHostRecordHeader {
            flags: self.flags,
            mac_address: self.mac_address.into_array(),
            ip_version: self.ip_address.version(),
            ip_address: self.ip_address.encode(),
            port_count: port_count.to_be(),
        };

        buf.put_slice(header.as_bytes());

        for port in &self.ports {
            buf.put_u16(*port);
        }
    }

    #[inline]
    fn size(&self) -> usize {
        std::mem::size_of::<RawHostRecordHeader>() + (self.ports.len() << 1)
    }
}

/// Host record header.
#[repr(C, packed)]
#[derive(Copy, Clone)]
struct RawHostRecordHeader {
    flags: u8,
    mac_address: [u8; 6],
    ip_version: u8,
    ip_address: [u8; 16],
    port_count: u16,
}

impl AsBytes for RawHostRecordHeader {}
impl FromBytes for RawHostRecordHeader {}
