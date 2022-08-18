pub mod net;

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

use bytes::{
    buf::{Buf, BufMut, Take},
    Bytes,
};

/// Trait for types that can be represented as a slice of bytes.
pub trait AsBytes: Sized + Copy {
    /// Get this value as a slice of bytes.
    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const _ as *const u8, self.size()) }
    }

    /// Get size of this value in bytes.
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Unexpected EOF.
#[derive(Debug, Copy, Clone)]
pub struct UnexpectedEof;

impl Display for UnexpectedEof {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("unexpected EOF")
    }
}

impl Error for UnexpectedEof {}

/// Trait for types that can be read directly from a slice of bytes.
pub trait FromBytes: Sized + Copy {
    /// Get a reference to `Self` from a given slice of bytes.
    ///
    /// Note that this method does not copy anything.
    fn from_bytes(data: &[u8]) -> Result<&Self, UnexpectedEof> {
        if data.len() < std::mem::size_of::<Self>() {
            return Err(UnexpectedEof);
        }

        let res = unsafe { &*(data.as_ptr() as *const _ as *const Self) };

        Ok(res)
    }
}

/// Trait for types that can be encoded as a series of bytes.
pub trait Encode {
    /// Encode the object.
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut;

    /// Get number of bytes of the encoded object.
    fn size(&self) -> usize;
}

/// Trait for types that can be decoded from their byte representation.
pub trait Decode: Sized {
    type Error;

    /// Decode the next object.
    fn decode<B>(data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>;
}

/// Trait for types that can be decoded from their byte representation.
pub trait DecodeWithContext: Sized {
    type Context;
    type Error;

    /// Decode the next object.
    fn decode<B>(cx: Self::Context, data: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf + AsRef<[u8]>;
}

/// Extensions for the byte buffer.
pub trait BufExt: Buf + AsRef<[u8]> + Sized {
    /// Extended take for continuous buffers.
    fn take_ext(self, limit: usize) -> TakeExt<Self>;
}

impl<T> BufExt for T
where
    T: Buf + AsRef<[u8]>,
{
    fn take_ext(self, limit: usize) -> TakeExt<Self> {
        TakeExt {
            inner: self.take(limit),
        }
    }
}

/// Extended take for continuous buffers.
pub struct TakeExt<T> {
    inner: Take<T>,
}

impl<T> AsRef<[u8]> for TakeExt<T>
where
    T: AsRef<[u8]>,
{
    fn as_ref(&self) -> &[u8] {
        self.inner.get_ref().as_ref()
    }
}

impl<T> Buf for TakeExt<T>
where
    T: Buf,
{
    fn remaining(&self) -> usize {
        self.inner.remaining()
    }

    fn chunk(&self) -> &[u8] {
        self.inner.chunk()
    }

    fn advance(&mut self, cnt: usize) {
        self.inner.advance(cnt)
    }

    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        self.inner.copy_to_bytes(len)
    }
}
