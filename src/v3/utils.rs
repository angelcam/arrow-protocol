/// Trait for converting `Copy` type instances to byte slices.
pub trait AsBytes {
    /// Get the value as a byte slice.
    fn as_bytes(&self) -> &[u8];
}

impl<T> AsBytes for T
where
    T: Copy,
{
    fn as_bytes(&self) -> &[u8] {
        let ptr = self as *const T;
        let size = std::mem::size_of_val(self);

        // SAFETY: This is safe because T is `Copy` and we are creating a byte
        //   slice from a valid pointer with the correct size.
        unsafe { std::slice::from_raw_parts(ptr as _, size) }
    }
}

impl<T> AsBytes for [T]
where
    T: Copy,
{
    fn as_bytes(&self) -> &[u8] {
        let ptr = self.as_ptr();
        let size = std::mem::size_of_val(self);

        // SAFETY: This is safe because T is `Copy` and we are creating a byte
        //   slice from a valid pointer with the correct size.
        unsafe { std::slice::from_raw_parts(ptr as _, size) }
    }
}

/// Trait for creating `Copy` type instances from bytes.
pub trait FromBytes {
    /// Copy the value instance from bytes.
    ///
    /// # Panics
    /// The method will panic if the provided byte slice is smaller than the
    /// size of the type.
    fn from_bytes(bytes: &[u8]) -> Self;
}

impl<T> FromBytes for T
where
    T: Copy,
{
    fn from_bytes(bytes: &[u8]) -> Self {
        let size = std::mem::size_of::<T>();

        assert!(bytes.len() >= size);

        // SAFETY: This is safe because T is `Copy` and we are reading from a
        //   valid pointer with the correct size.
        unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const T) }
    }
}

#[cfg(test)]
pub mod tests {
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    use bytes::Bytes;
    use futures::channel::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio_util::io::StreamReader;

    use crate::v3::{
        msg::{EncodeMessage, EncodedMessage, Message, MessageEncoder, MessageKind},
        utils::FromBytes,
    };

    /// Type alias.
    pub type FakeIoInput = Receiver<io::Result<Bytes>>;

    /// Type alias.
    pub type FakeIoInputTx = Sender<io::Result<Bytes>>;

    /// Type alias.
    pub type FakeIoOutput = UnboundedSender<Bytes>;

    /// Type alias.
    pub type FakeIoOutputRx = UnboundedReceiver<Bytes>;

    /// Create a new fake IO input channel.
    pub fn create_fake_io_input(buffer: usize) -> (FakeIoInputTx, FakeIoInput) {
        mpsc::channel(buffer)
    }

    /// Create a new fake IO output channel.
    pub fn create_fake_io_output() -> (FakeIoOutput, FakeIoOutputRx) {
        mpsc::unbounded()
    }

    /// Fake IO for testing.
    pub struct FakeIo {
        rx: StreamReader<Receiver<io::Result<Bytes>>, Bytes>,
        tx: Option<UnboundedSender<Bytes>>,
    }

    impl FakeIo {
        /// Create a new fake IO.
        pub fn new(rx: FakeIoInput, tx: Option<FakeIoOutput>) -> Self {
            Self {
                rx: StreamReader::new(rx),
                tx,
            }
        }
    }

    impl AsyncRead for FakeIo {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            AsyncRead::poll_read(Pin::new(&mut self.rx), cx, buf)
        }
    }

    impl AsyncWrite for FakeIo {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            mut buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            // helper struct
            #[repr(C, packed)]
            #[derive(Copy, Clone)]
            struct MessageHeader {
                version: u8,
                kind: u8,
                length: u32,
            }

            let len = buf.len();

            let Some(tx) = self.tx.as_mut() else {
                return Poll::Ready(Ok(len));
            };

            // NOTE: The BaseConnection may send multiple messages in one write
            //   call, so we need to split them here for testing purposes.
            while buf.len() >= std::mem::size_of::<MessageHeader>() {
                let header = MessageHeader::from_bytes(&buf);

                let header_len = std::mem::size_of::<MessageHeader>();
                let payload_len = u32::from_be(header.length) as usize;

                let total_len = header_len + payload_len;

                if buf.len() < total_len {
                    break;
                }

                let _ = tx.unbounded_send(Bytes::copy_from_slice(&buf[..total_len]));

                buf = &buf[total_len..];
            }

            // forward any remaining data
            if !buf.is_empty() {
                let _ = tx.unbounded_send(Bytes::copy_from_slice(buf));
            }

            Poll::Ready(Ok(len))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Create a new encoded message with a given payload.
    pub fn create_message(kind: MessageKind, payload: &[u8]) -> EncodedMessage {
        EncodedMessage::new(kind, Bytes::copy_from_slice(payload))
    }

    /// Helper trait.
    pub trait MessageExt {
        /// Convert the message into an encoded message.
        fn to_encoded_message(&self) -> EncodedMessage;
    }

    impl<T> MessageExt for T
    where
        T: Message + EncodeMessage,
    {
        fn to_encoded_message(&self) -> EncodedMessage {
            let mut encoder = MessageEncoder::new();

            encoder.encode(self)
        }
    }

    /// Helper trait.
    pub trait EncodedMessageExt {
        /// Convert the message to bytes.
        fn to_bytes(&self) -> Bytes;
    }

    impl EncodedMessageExt for EncodedMessage {
        fn to_bytes(&self) -> Bytes {
            let kind = self.kind();
            let data = self.data();

            let mut buf = Vec::new();

            buf.extend_from_slice(&[0x03, kind as u8]);
            buf.extend_from_slice(&u32::to_be_bytes(data.len() as u32));
            buf.extend_from_slice(data);

            buf.into()
        }
    }
}
