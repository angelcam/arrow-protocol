pub mod v2;
pub mod v3;

use uuid::Uuid;

pub use macaddr::MacAddr6 as MacAddr;

/// Current version of the Arrow protocol.
pub const ARROW_PROTOCOL_VERSION: u8 = 3;

/// Check if a given version of the protocol is supported.
#[inline]
pub const fn is_supported_version(version: u8) -> bool {
    version <= ARROW_PROTOCOL_VERSION
}

/// Arrow Client ID.
pub type ClientId = Uuid;

/// Arrow Client secret key.
pub type ClientKey = [u8; 16];
