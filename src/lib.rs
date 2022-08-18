mod utils;

pub mod msg;

use uuid::Uuid;

pub use macaddr::MacAddr6 as MacAddr;

pub use self::{
    msg::{ArrowMessage, ArrowMessageBody, ControlMessage, ControlMessagePayload},
    utils::{Decode, DecodeWithContext, Encode},
};

#[cfg(feature = "codec")]
pub use self::msg::ArrowProtocolCodec;

#[cfg(feature = "stream")]
pub use self::msg::ArrowMessageStream;

/// Current version of the Arrow protocol.
pub const ARROW_PROTOCOL_VERSION: u8 = 2;

/// Check if a given version of the protocol is supported.
#[inline]
pub const fn is_supported_version(version: u8) -> bool {
    version <= ARROW_PROTOCOL_VERSION
}

/// Arrow Client ID.
pub type ClientId = Uuid;

/// Arrow Client secret key.
pub type ClientKey = [u8; 16];
