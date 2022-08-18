use std::io;

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::{ArrowMessage, Decode, Encode};

/// Arrow Protocol codec.
pub struct ArrowProtocolCodec(());

impl ArrowProtocolCodec {
    /// Create a new encoder/decoder for the Arrow Protocol.
    #[inline]
    pub const fn new() -> Self {
        Self(())
    }
}

impl Default for ArrowProtocolCodec {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for ArrowProtocolCodec {
    type Item = ArrowMessage;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match ArrowMessage::decode(buf) {
            Ok(msg) => Ok(Some(msg)),
            Err(err) => {
                if err.is_incomplete() {
                    Ok(None)
                } else {
                    Err(io::Error::new(io::ErrorKind::InvalidData, err))
                }
            }
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let msg = self.decode(buf)?;

        if msg.is_some() {
            Ok(msg)
        } else if buf.is_empty() {
            Ok(None)
        } else {
            Err(io::Error::from(io::ErrorKind::UnexpectedEof))
        }
    }
}

impl Encoder<ArrowMessage> for ArrowProtocolCodec {
    type Error = io::Error;

    fn encode(&mut self, item: ArrowMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(item.size());

        item.encode(dst);

        Ok(())
    }
}
