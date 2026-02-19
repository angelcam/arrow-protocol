//! Scan report.

use std::net::IpAddr;

use bytes::{Buf, BufMut};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, SizeError, Unaligned,
    byteorder::network_endian::{U16, U32},
};

use crate::{
    MacAddr,
    v2::{
        msg::control::{DecodingError, svc_table::ServiceTable},
        utils::{Decode, Encode, UnexpectedEof, net::IpAddrExt},
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
        let (header, _) = RawMessageHeader::ref_from_prefix(data.as_ref())
            .map_err(SizeError::from)
            .map_err(|_| UnexpectedEof)?;

        let request_id = header.request_id.get();
        let host_count = header.host_count.get() as usize;

        data.advance(std::mem::size_of::<RawMessageHeader>());

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
            request_id: U16::new(self.request_id),
            host_count: U32::new(host_count),
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
#[derive(Copy, Clone, KnownLayout, Immutable, Unaligned, IntoBytes, FromBytes)]
#[repr(C)]
struct RawMessageHeader {
    request_id: U16,
    host_count: U32,
}

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
        let (header, _) = RawHostRecordHeader::ref_from_prefix(data.as_ref())
            .map_err(SizeError::from)
            .map_err(|_| UnexpectedEof)?;

        let header_len = std::mem::size_of::<RawHostRecordHeader>();

        let ip_address = IpAddr::decode(header.ip_version, header.ip_address)?;

        let port_count = header.port_count.get() as usize;

        let rest = &data.as_ref()[header_len..];

        let (be_ports, _) = <[U16]>::ref_from_prefix_with_elems(rest, port_count)
            .map_err(SizeError::from)
            .map_err(|_| UnexpectedEof)?;

        let ports = be_ports.iter().copied().map(U16::get).collect();

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
            port_count: U16::new(port_count),
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
#[derive(Copy, Clone, KnownLayout, Immutable, Unaligned, IntoBytes, FromBytes)]
#[repr(C)]
struct RawHostRecordHeader {
    flags: u8,
    mac_address: [u8; 6],
    ip_version: u8,
    ip_address: [u8; 16],
    port_count: U16,
}
