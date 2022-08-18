//! Arrow Protocol messages.

#[cfg(feature = "codec")]
mod codec;

#[cfg(feature = "stream")]
pub mod stream;

pub mod control;

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

use bytes::{Buf, BufMut, Bytes};

use self::control::InvalidControlMessage;

use crate::{
    utils::{AsBytes, BufExt, Decode, DecodeWithContext, Encode, FromBytes, UnexpectedEof},
    ARROW_PROTOCOL_VERSION,
};

pub use self::control::{ControlMessage, ControlMessagePayload};

#[cfg(feature = "codec")]
pub use self::codec::ArrowProtocolCodec;

#[cfg(feature = "stream")]
pub use self::stream::ArrowMessageStream;

/// Invalid message.
#[derive(Debug, Clone)]
pub enum InvalidMessage {
    IncompleteMessage,
    UnsupportedProtocolVersion(u8),
    InvalidControlMessage(InvalidControlMessage),
}

impl InvalidMessage {
    /// Check if more data is required to decode the message.
    pub fn is_incomplete(&self) -> bool {
        matches!(self, Self::IncompleteMessage)
    }
}

impl Display for InvalidMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompleteMessage => f.write_str("incomplete message"),
            Self::UnsupportedProtocolVersion(v) => write!(f, "unsupported protocol version: {}", v),
            Self::InvalidControlMessage(err) => write!(f, "invalid control message: {}", err),
        }
    }
}

impl Error for InvalidMessage {}

impl From<UnexpectedEof> for InvalidMessage {
    fn from(_: UnexpectedEof) -> Self {
        Self::IncompleteMessage
    }
}

impl From<InvalidControlMessage> for InvalidMessage {
    fn from(err: InvalidControlMessage) -> Self {
        Self::InvalidControlMessage(err)
    }
}

/// Arrow Protocol message.
#[derive(Clone)]
pub struct ArrowMessage {
    version: u8,
    service_id: u16,
    session_id: u32,
    body: ArrowMessageBody,
}

impl ArrowMessage {
    /// Create a new message containing a given payload.
    pub fn new_data_message(service_id: u16, session_id: u32, body: Bytes) -> Self {
        assert!(service_id > 0);
        assert!((session_id >> 24) == 0);
        assert!(body.len() < (u32::MAX as usize));

        Self {
            version: ARROW_PROTOCOL_VERSION,
            service_id,
            session_id,
            body: body.into(),
        }
    }

    /// Get version of the Arrow Protocol.
    #[inline]
    pub fn version(&self) -> u8 {
        self.version
    }

    /// Set version of the Arrow Protocol.
    #[inline]
    pub fn with_version(mut self, version: u8) -> Self {
        self.version = version;
        self
    }

    /// Get the service ID.
    #[inline]
    pub fn service_id(&self) -> u16 {
        self.service_id
    }

    /// Set service ID.
    #[inline]
    pub fn with_service_id(mut self, service_id: u16) -> Self {
        self.service_id = service_id;
        self
    }

    /// Get the session ID.
    #[inline]
    pub fn session_id(&self) -> u32 {
        self.session_id
    }

    /// Set session ID.
    #[inline]
    pub fn with_session_id(mut self, session_id: u32) -> Self {
        self.session_id = session_id;
        self
    }

    /// Get the message body.
    #[inline]
    pub fn body(&self) -> &ArrowMessageBody {
        &self.body
    }

    /// Set message body.
    #[inline]
    pub fn with_body<T>(mut self, body: T) -> Self
    where
        T: Into<ArrowMessageBody>,
    {
        self.body = body.into();
        self
    }

    /// Take the message body.
    #[inline]
    pub fn into_body(self) -> ArrowMessageBody {
        self.body
    }
}

impl Decode for ArrowMessage {
    type Error = InvalidMessage;

    fn decode<B>(data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        let buf = data.as_ref();

        if buf.is_empty() {
            return Err(InvalidMessage::IncompleteMessage);
        } else if !crate::is_supported_version(buf[0]) {
            return Err(InvalidMessage::UnsupportedProtocolVersion(buf[0]));
        }

        let header = RawMessageHeader::from_bytes(buf)?;

        let header_size = header.size();

        let version = header.version;
        let service_id = u16::from_be(header.service_id);
        let session_id = u32::from_be(header.session_id) & 0xffffff;
        let body_size = u32::from_be(header.size) as usize;

        let message_size = header_size + body_size;

        if data.remaining() < message_size {
            return Err(InvalidMessage::IncompleteMessage);
        }

        data.advance(header_size);

        let context = ArrowMessageContext::new(version, service_id);

        let mut body_data = data.take_ext(body_size);

        let body = ArrowMessageBody::decode(context, &mut body_data)?;

        body_data.advance(body_data.remaining());

        let res = ArrowMessage {
            version,
            service_id,
            session_id,
            body,
        };

        Ok(res)
    }
}

impl Encode for ArrowMessage {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        let body_size = self.body.size() as u32;

        let header = RawMessageHeader {
            version: self.version,
            service_id: self.service_id.to_be(),
            session_id: self.session_id.to_be(),
            size: body_size.to_be(),
        };

        buf.put_slice(header.as_bytes());

        self.body.encode(buf);
    }

    #[inline]
    fn size(&self) -> usize {
        std::mem::size_of::<RawMessageHeader>() + self.body.size()
    }
}

impl From<ControlMessage> for ArrowMessage {
    #[inline]
    fn from(msg: ControlMessage) -> Self {
        Self {
            version: ARROW_PROTOCOL_VERSION,
            service_id: 0,
            session_id: 0,
            body: msg.into(),
        }
    }
}

/// Arrow Message body.
#[derive(Clone)]
pub enum ArrowMessageBody {
    Data(Bytes),
    ControlMessage(ControlMessage),
}

impl DecodeWithContext for ArrowMessageBody {
    type Context = ArrowMessageContext;
    type Error = InvalidMessage;

    fn decode<B>(context: ArrowMessageContext, data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>,
    {
        if context.service_id == 0 {
            Ok(Self::ControlMessage(ControlMessage::decode(
                context.version,
                data,
            )?))
        } else {
            Ok(Self::Data(data.copy_to_bytes(data.remaining())))
        }
    }
}

impl Encode for ArrowMessageBody {
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        match self {
            Self::Data(data) => buf.put_slice(data),
            Self::ControlMessage(msg) => msg.encode(buf),
        }
    }

    fn size(&self) -> usize {
        match self {
            Self::Data(data) => data.len(),
            Self::ControlMessage(msg) => msg.size(),
        }
    }
}

impl From<Bytes> for ArrowMessageBody {
    #[inline]
    fn from(data: Bytes) -> Self {
        Self::Data(data)
    }
}

impl From<ControlMessage> for ArrowMessageBody {
    #[inline]
    fn from(msg: ControlMessage) -> Self {
        Self::ControlMessage(msg)
    }
}

/// Arrow Message header.
#[repr(packed)]
#[derive(Copy, Clone)]
struct RawMessageHeader {
    version: u8,
    service_id: u16,
    session_id: u32,
    size: u32,
}

impl AsBytes for RawMessageHeader {}
impl FromBytes for RawMessageHeader {}

/// Arrow Message decoding context.
#[derive(Copy, Clone)]
pub struct ArrowMessageContext {
    version: u8,
    service_id: u16,
}

impl ArrowMessageContext {
    /// Create a new context.
    #[inline]
    pub const fn new(version: u8, service_id: u16) -> Self {
        Self {
            version,
            service_id,
        }
    }
}
