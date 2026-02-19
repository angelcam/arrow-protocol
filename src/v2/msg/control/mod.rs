//! Arrow Control Protocol messages.

mod ack;
mod connect;
mod data_ack;
mod hup;
mod redirect;
mod register;
mod status;
mod update;

pub mod scan_report;
pub mod svc_table;

use std::{
    borrow::Cow,
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

use bytes::{Buf, BufMut};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, SizeError, Unaligned,
    byteorder::network_endian::U16,
};

pub use self::{
    ack::AckMessage, connect::ConnectMessage, data_ack::DataAckMessage, hup::HupMessage,
    redirect::RedirectMessage, register::RegisterMessage, scan_report::ScanReportMessage,
    status::StatusMessage, svc_table::ServiceTable, update::UpdateMessage,
};

use crate::{
    ClientId, ClientKey, MacAddr,
    v2::{
        msg::control::scan_report::HostRecord,
        utils::{Decode, DecodeWithContext, Encode, UnexpectedEof, net::UnsupportedIpAddrVersion},
    },
};

/// Decoding error.
#[derive(Debug, Clone)]
pub enum DecodingError {
    UnexpectedEof,
    InvalidData(Cow<'static, str>),
    UnsupportedProtocolVersion(u8),
    UnsupportedControlMessage(ControlMessageType),
}

impl DecodingError {
    /// Create a new invalid data error with a given message.
    fn invalid_data<T>(msg: T) -> Self
    where
        T: Into<Cow<'static, str>>,
    {
        Self::InvalidData(msg.into())
    }
}

impl Display for DecodingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => f.write_str("unexpected eof"),
            Self::InvalidData(msg) => write!(f, "invalid data: {}", msg),
            Self::UnsupportedProtocolVersion(v) => write!(f, "unsupported protocol version: {}", v),
            Self::UnsupportedControlMessage(t) => {
                write!(f, "unsupported control message: {}", t.as_str())
            }
        }
    }
}

impl Error for DecodingError {}

impl From<UnexpectedEof> for DecodingError {
    fn from(_: UnexpectedEof) -> Self {
        Self::UnexpectedEof
    }
}

impl From<UnsupportedIpAddrVersion> for DecodingError {
    fn from(_: UnsupportedIpAddrVersion) -> Self {
        Self::InvalidData(Cow::Borrowed("unsupported IP address version"))
    }
}

/// Invalid control message.
#[derive(Clone)]
pub struct InvalidControlMessage {
    inner: InvalidControlMessageInner,
}

impl Debug for InvalidControlMessage {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for InvalidControlMessage {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl Error for InvalidControlMessage {}

impl From<InvalidControlMessageInner> for InvalidControlMessage {
    fn from(err: InvalidControlMessageInner) -> Self {
        Self { inner: err }
    }
}

impl From<UnexpectedEof> for InvalidControlMessage {
    fn from(err: UnexpectedEof) -> Self {
        Self::from(InvalidControlMessageInner::from(err))
    }
}

impl From<UnknownControlMessage> for InvalidControlMessage {
    #[inline]
    fn from(err: UnknownControlMessage) -> Self {
        Self::from(InvalidControlMessageInner::from(err))
    }
}

/// Invalid control message variants.
#[derive(Clone)]
enum InvalidControlMessageInner {
    UnexpectedEof,
    UnknownControlMessage(u16),
    InnerDecodingError(ControlMessageType, DecodingError),
}

impl Display for InvalidControlMessageInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => f.write_str("unexpected EOF"),
            Self::UnknownControlMessage(t) => write!(f, "unknown message type: 0x{:04x}", t),
            Self::InnerDecodingError(t, err) => {
                write!(f, "invalid {} message: {}", t.as_str(), err)
            }
        }
    }
}

impl From<UnexpectedEof> for InvalidControlMessageInner {
    fn from(_: UnexpectedEof) -> Self {
        Self::UnexpectedEof
    }
}

impl From<UnknownControlMessage> for InvalidControlMessageInner {
    fn from(err: UnknownControlMessage) -> Self {
        Self::UnknownControlMessage(err.0)
    }
}

/// Arrow Control Protocol message.
#[derive(Clone)]
pub struct ControlMessage {
    message_id: u16,
    message_payload: ControlMessagePayload,
}

impl ControlMessage {
    /// Create a new control message with a given payload.
    pub fn new<T>(message_id: u16, payload: T) -> Self
    where
        T: Into<ControlMessagePayload>,
    {
        Self {
            message_id,
            message_payload: payload.into(),
        }
    }

    /// Create a new ACK message.
    #[inline]
    pub fn new_ack_message(message_id: u16, code: u32) -> Self {
        Self::new(message_id, AckMessage::new(code))
    }

    /// Create a new ping message.
    #[inline]
    pub fn new_ping_message(message_id: u16) -> Self {
        Self::new(message_id, ControlMessagePayload::Ping)
    }

    /// Create a new register message.
    #[inline]
    pub fn new_register_message(
        message_id: u16,
        client_id: ClientId,
        client_key: ClientKey,
        mac_address: MacAddr,
        service_table: ServiceTable,
    ) -> Self {
        Self::new(
            message_id,
            RegisterMessage::new(client_id, mac_address, client_key, service_table),
        )
    }

    /// Create a new register message.
    #[inline]
    pub fn new_register_message_v2(
        message_id: u16,
        client_id: ClientId,
        client_key: ClientKey,
        mac_address: MacAddr,
        service_table: ServiceTable,
    ) -> Self {
        Self::new(
            message_id,
            RegisterMessage::new_v2(client_id, mac_address, client_key, service_table),
        )
    }

    /// Create a new redirect message.
    pub fn new_redirect_message<T>(message_id: u16, address: T) -> Self
    where
        T: ToString,
    {
        Self::new(message_id, RedirectMessage::new(address))
    }

    /// Create a new update message.
    #[inline]
    pub fn new_update_message(message_id: u16, service_table: ServiceTable) -> Self {
        Self::new(message_id, UpdateMessage::new(service_table))
    }

    /// Create a new HUP message.
    #[inline]
    pub fn new_hup_message(message_id: u16, session_id: u32, error_code: u32) -> Self {
        Self::new(message_id, HupMessage::new(session_id, error_code))
    }

    /// Create a new reset service table message.
    #[inline]
    pub fn new_reset_service_table_message(message_id: u16) -> Self {
        Self::new(message_id, ControlMessagePayload::ResetServiceTable)
    }

    /// Create a new scan network message.
    #[inline]
    pub fn new_scan_network_message(message_id: u16) -> Self {
        Self::new(message_id, ControlMessagePayload::ScanNetwork)
    }

    /// Create a new get status message.
    #[inline]
    pub fn new_get_status_message(message_id: u16) -> Self {
        Self::new(message_id, ControlMessagePayload::GetStatus)
    }

    /// Create a new status message.
    #[inline]
    pub fn new_status_message(
        message_id: u16,
        request_id: u16,
        status_flags: u32,
        active_sessions: u32,
    ) -> Self {
        Self::new(
            message_id,
            StatusMessage::new(request_id, status_flags, active_sessions),
        )
    }

    /// Create a new get scan report message.
    #[inline]
    pub fn new_get_scan_report_message(message_id: u16) -> Self {
        Self::new(message_id, ControlMessagePayload::GetScanReport)
    }

    /// Create a new scan report message.
    pub fn new_scan_report_message<T>(
        message_id: u16,
        request_id: u16,
        hosts: T,
        service_table: ServiceTable,
    ) -> Self
    where
        T: Into<Vec<HostRecord>>,
    {
        Self::new(
            message_id,
            ScanReportMessage::new(request_id, hosts, service_table),
        )
    }

    /// Create a new connect message.
    #[inline]
    pub fn new_connect_message(message_id: u16, service_id: u16, session_id: u32) -> Self {
        Self::new(message_id, ConnectMessage::new(service_id, session_id))
    }

    /// Create a new data ACK message.
    #[inline]
    pub fn new_data_ack_message(message_id: u16, session_id: u32, length: u32) -> Self {
        Self::new(message_id, DataAckMessage::new(session_id, length))
    }

    /// Get the message ID.
    #[inline]
    pub fn message_id(&self) -> u16 {
        self.message_id
    }

    /// Set the message ID.
    #[inline]
    pub fn with_message_id(mut self, id: u16) -> Self {
        self.message_id = id;
        self
    }

    /// Get the message type.
    #[inline]
    pub fn message_type(&self) -> ControlMessageType {
        self.message_payload.message_type()
    }

    /// Check if this is a response message.
    #[inline]
    pub fn is_response(&self) -> bool {
        self.message_payload.is_response_message()
    }

    /// Get the message payload.
    #[inline]
    pub fn payload(&self) -> &ControlMessagePayload {
        &self.message_payload
    }

    /// Set message payload.
    pub fn with_payload<T>(mut self, payload: T) -> Self
    where
        T: Into<ControlMessagePayload>,
    {
        self.message_payload = payload.into();
        self
    }

    /// Take the message payload.
    #[inline]
    pub fn into_payload(self) -> ControlMessagePayload {
        self.message_payload
    }
}

impl DecodeWithContext for ControlMessage {
    type Context = u8;
    type Error = InvalidControlMessage;

    fn decode<B>(version: u8, data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        let (header, _) = RawMessageHeader::ref_from_prefix(data.as_ref())
            .map_err(SizeError::from)
            .map_err(|_| UnexpectedEof)?;

        let message_id = header.message_id.get();
        let message_type = ControlMessageType::try_from(header.message_type.get())?;

        let context = ControlMessageContext::new(version, message_type);

        data.advance(std::mem::size_of::<RawMessageHeader>());

        let message_payload = ControlMessagePayload::decode(context, data)
            .map_err(|err| InvalidControlMessageInner::InnerDecodingError(message_type, err))?;

        let res = Self {
            message_id,
            message_payload,
        };

        Ok(res)
    }
}

impl Encode for ControlMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        let message_type = self.message_payload.message_type() as u16;

        let header = RawMessageHeader {
            message_id: U16::new(self.message_id),
            message_type: U16::new(message_type),
        };

        buf.put_slice(header.as_bytes());

        self.message_payload.encode(buf);
    }

    #[inline]
    fn size(&self) -> usize {
        std::mem::size_of::<RawMessageHeader>() + self.message_payload.size()
    }
}

/// Control Protocol message header.
#[derive(Copy, Clone, KnownLayout, Immutable, Unaligned, IntoBytes, FromBytes)]
#[repr(C)]
struct RawMessageHeader {
    message_id: U16,
    message_type: U16,
}

/// Unknown control message type.
#[derive(Debug, Copy, Clone)]
pub struct UnknownControlMessage(u16);

impl Display for UnknownControlMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "unknown control message type: 0x{:04x}", self.0)
    }
}

impl Error for UnknownControlMessage {}

/// Control Protocol message type.
#[repr(u16)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ControlMessageType {
    Ack = 0x00,
    Ping = 0x01,
    Register = 0x02,
    Redirect = 0x03,
    Update = 0x04,
    Hup = 0x05,
    ResetServiceTable = 0x06,
    ScanNetwork = 0x07,
    GetStatus = 0x08,
    Status = 0x09,
    GetScanReport = 0x0a,
    ScanReport = 0x0b,
    DataAck = 0x0c,
    Connect = 0x0d,
}

impl ControlMessageType {
    /// Get the control message type name.
    pub fn as_str(&self) -> &'static str {
        match self {
            ControlMessageType::Ack => "ACK",
            ControlMessageType::Ping => "PING",
            ControlMessageType::Register => "REGISTER",
            ControlMessageType::Redirect => "REDIRECT",
            ControlMessageType::Update => "UPDATE",
            ControlMessageType::Hup => "HUP",
            ControlMessageType::ResetServiceTable => "RESET_SERVICE_TABLE",
            ControlMessageType::ScanNetwork => "SCAN_NETWORK",
            ControlMessageType::GetStatus => "GET_STATUS",
            ControlMessageType::Status => "STATUS",
            ControlMessageType::GetScanReport => "GET_SCAN_REPORT",
            ControlMessageType::ScanReport => "SCAN_REPORT",
            ControlMessageType::DataAck => "DATA_ACK",
            ControlMessageType::Connect => "CONNECT",
        }
    }
}

impl TryFrom<u16> for ControlMessageType {
    type Error = UnknownControlMessage;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        let res = match value {
            0x00 => Self::Ack,
            0x01 => Self::Ping,
            0x02 => Self::Register,
            0x03 => Self::Redirect,
            0x04 => Self::Update,
            0x05 => Self::Hup,
            0x06 => Self::ResetServiceTable,
            0x07 => Self::ScanNetwork,
            0x08 => Self::GetStatus,
            0x09 => Self::Status,
            0x0a => Self::GetScanReport,
            0x0b => Self::ScanReport,
            0x0c => Self::DataAck,
            0x0d => Self::Connect,
            _ => return Err(UnknownControlMessage(value)),
        };

        Ok(res)
    }
}

/// Control protocol message payload.
#[derive(Clone)]
pub enum ControlMessagePayload {
    Ack(AckMessage),
    Ping,
    Register(RegisterMessage),
    Redirect(RedirectMessage),
    Update(UpdateMessage),
    Hup(HupMessage),
    ResetServiceTable,
    ScanNetwork,
    GetStatus,
    Status(StatusMessage),
    GetScanReport,
    ScanReport(ScanReportMessage),
    DataAck(DataAckMessage),
    Connect(ConnectMessage),
}

impl ControlMessagePayload {
    /// Get the message type.
    fn message_type(&self) -> ControlMessageType {
        match self {
            Self::Ack(_) => ControlMessageType::Ack,
            Self::Ping => ControlMessageType::Ping,
            Self::Register(_) => ControlMessageType::Register,
            Self::Redirect(_) => ControlMessageType::Redirect,
            Self::Update(_) => ControlMessageType::Update,
            Self::Hup(_) => ControlMessageType::Hup,
            Self::ResetServiceTable => ControlMessageType::ResetServiceTable,
            Self::ScanNetwork => ControlMessageType::ScanNetwork,
            Self::GetStatus => ControlMessageType::GetStatus,
            Self::Status(_) => ControlMessageType::Status,
            Self::GetScanReport => ControlMessageType::GetScanReport,
            Self::ScanReport(_) => ControlMessageType::ScanReport,
            Self::DataAck(_) => ControlMessageType::DataAck,
            Self::Connect(_) => ControlMessageType::Connect,
        }
    }

    /// Check if this is a response message type.
    fn is_response_message(&self) -> bool {
        matches!(self, Self::Ack(_) | Self::Status(_) | Self::ScanReport(_))
    }
}

impl DecodeWithContext for ControlMessagePayload {
    type Context = ControlMessageContext;
    type Error = DecodingError;

    fn decode<B>(context: ControlMessageContext, data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        let version = context.version;

        let res = match context.message_type {
            ControlMessageType::Ack => Self::Ack(AckMessage::decode(data)?),
            ControlMessageType::Ping => Self::Ping,
            ControlMessageType::Register => Self::Register(RegisterMessage::decode(version, data)?),
            ControlMessageType::Redirect => Self::Redirect(RedirectMessage::decode(data)?),
            ControlMessageType::Update => Self::Update(UpdateMessage::decode(data)?),
            ControlMessageType::Hup => Self::Hup(HupMessage::decode(data)?),
            ControlMessageType::ResetServiceTable => Self::ResetServiceTable,
            ControlMessageType::ScanNetwork => Self::ScanNetwork,
            ControlMessageType::GetStatus => Self::GetStatus,
            ControlMessageType::Status => Self::Status(StatusMessage::decode(data)?),
            ControlMessageType::GetScanReport => Self::GetScanReport,
            ControlMessageType::ScanReport => Self::ScanReport(ScanReportMessage::decode(data)?),
            ControlMessageType::DataAck if version > 1 => {
                Self::DataAck(DataAckMessage::decode(data)?)
            }
            ControlMessageType::Connect if version > 1 => {
                Self::Connect(ConnectMessage::decode(data)?)
            }
            t => return Err(DecodingError::UnsupportedControlMessage(t)),
        };

        Ok(res)
    }
}

impl Encode for ControlMessagePayload {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        match self {
            ControlMessagePayload::Ack(payload) => payload.encode(buf),
            ControlMessagePayload::Register(payload) => payload.encode(buf),
            ControlMessagePayload::Redirect(payload) => payload.encode(buf),
            ControlMessagePayload::Update(payload) => payload.encode(buf),
            ControlMessagePayload::Hup(payload) => payload.encode(buf),
            ControlMessagePayload::Status(payload) => payload.encode(buf),
            ControlMessagePayload::ScanReport(payload) => payload.encode(buf),
            ControlMessagePayload::DataAck(payload) => payload.encode(buf),
            ControlMessagePayload::Connect(payload) => payload.encode(buf),
            _ => (),
        }
    }

    fn size(&self) -> usize {
        match self {
            Self::Ack(payload) => payload.size(),
            Self::Register(payload) => payload.size(),
            Self::Redirect(payload) => payload.size(),
            Self::Update(payload) => payload.size(),
            Self::Hup(payload) => payload.size(),
            Self::Status(payload) => payload.size(),
            Self::ScanReport(payload) => payload.size(),
            Self::DataAck(payload) => payload.size(),
            Self::Connect(payload) => payload.size(),
            _ => 0,
        }
    }
}

impl From<AckMessage> for ControlMessagePayload {
    #[inline]
    fn from(body: AckMessage) -> Self {
        Self::Ack(body)
    }
}

impl From<RegisterMessage> for ControlMessagePayload {
    #[inline]
    fn from(body: RegisterMessage) -> Self {
        Self::Register(body)
    }
}

impl From<RedirectMessage> for ControlMessagePayload {
    #[inline]
    fn from(body: RedirectMessage) -> Self {
        Self::Redirect(body)
    }
}

impl From<UpdateMessage> for ControlMessagePayload {
    #[inline]
    fn from(body: UpdateMessage) -> Self {
        Self::Update(body)
    }
}

impl From<HupMessage> for ControlMessagePayload {
    #[inline]
    fn from(body: HupMessage) -> Self {
        Self::Hup(body)
    }
}

impl From<StatusMessage> for ControlMessagePayload {
    #[inline]
    fn from(body: StatusMessage) -> Self {
        Self::Status(body)
    }
}

impl From<ScanReportMessage> for ControlMessagePayload {
    #[inline]
    fn from(body: ScanReportMessage) -> Self {
        Self::ScanReport(body)
    }
}

impl From<DataAckMessage> for ControlMessagePayload {
    #[inline]
    fn from(body: DataAckMessage) -> Self {
        Self::DataAck(body)
    }
}

impl From<ConnectMessage> for ControlMessagePayload {
    #[inline]
    fn from(body: ConnectMessage) -> Self {
        Self::Connect(body)
    }
}

/// Control message decoding context.
#[derive(Copy, Clone)]
pub struct ControlMessageContext {
    version: u8,
    message_type: ControlMessageType,
}

impl ControlMessageContext {
    /// Create a new control message context.
    #[inline]
    pub const fn new(version: u8, message_type: ControlMessageType) -> Self {
        Self {
            version,
            message_type,
        }
    }
}
