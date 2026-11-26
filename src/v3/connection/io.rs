use std::{
    io::{self, IoSlice},
    mem::MaybeUninit,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::Sleep,
};

pin_project_lite::pin_project! {
    /// Base IO wrapper implementing lingering shutdown.
    #[project = BaseIOProjection]
    pub struct BaseIO<T> {
        #[pin]
        inner: T,
        reader: Option<Waker>,
        rx_state: RXState,
        tx_state: TXState,
        write_timeout: Duration,
        close_timeout: Duration,
        #[pin]
        tx_deadline: Option<Sleep>,
    }
}

impl<T> BaseIO<T> {
    /// Create a new base IO.
    pub const fn new(io: T, write_timeout: Duration, close_timeout: Duration) -> Self {
        Self {
            inner: io,
            reader: None,
            rx_state: RXState::Open,
            tx_state: TXState::Open,
            write_timeout,
            close_timeout,
            tx_deadline: None,
        }
    }

    /// Get the current TX channel state.
    pub fn tx_state(&self) -> TXState {
        self.tx_state
    }

    /// Common write operation wrapper.
    fn poll_write_with<F>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        f: F,
    ) -> Poll<io::Result<usize>>
    where
        F: FnOnce(&mut BaseIOProjection<T>, &mut Context<'_>) -> Poll<io::Result<usize>>,
    {
        let mut this = self.project();

        match *this.tx_state {
            TXState::Open => (),
            TXState::Closing => return Poll::Ready(Err(io::Error::closing_transport())),
            TXState::Closed | TXState::ClosedAndDrained => {
                return Poll::Ready(Err(io::Error::closed_transport()));
            }
            TXState::Broken => return Poll::Ready(Err(io::Error::broken_transport())),
        }

        let res = futures::ready!(f(&mut this, cx));

        if res.is_err() {
            this.mark_broken();
        }

        Poll::Ready(res)
    }
}

impl<T> AsyncRead for BaseIO<T>
where
    T: AsyncRead,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.project();

        if *this.rx_state == RXState::Closed {
            return Poll::Ready(Ok(()));
        } else if *this.tx_state == TXState::Broken {
            return Poll::Ready(Err(io::Error::broken_transport()));
        } else if *this.tx_state != TXState::Open {
            return Poll::Ready(Ok(()));
        }

        let remaining_before = buf.remaining();

        let Poll::Ready(res) = this.inner.poll_read(cx, buf) else {
            let task = cx.waker();

            *this.reader = Some(task.clone());

            return Poll::Pending;
        };

        if res.is_err() || (remaining_before > 0 && remaining_before == buf.remaining()) {
            *this.rx_state = RXState::Closed;
        }

        Poll::Ready(res)
    }
}

impl<T> AsyncWrite for BaseIO<T>
where
    T: AsyncRead + AsyncWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_with(cx, |this, cx| {
            this.poll_write_with(cx, |inner, cx| inner.poll_write(cx, buf))
        })
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_with(cx, |this, cx| {
            this.poll_write_with(cx, |inner, cx| inner.poll_write_vectored(cx, bufs))
        })
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();

        match *this.tx_state {
            TXState::Open | TXState::Closing => (),
            // there is nothing to flush if the TX channel is already closed
            TXState::Closed | TXState::ClosedAndDrained => return Poll::Ready(Ok(())),
            TXState::Broken => return Poll::Ready(Err(io::Error::broken_transport())),
        }

        let res = futures::ready!(this.poll_flush(cx));

        if res.is_err() {
            this.mark_broken();
        }

        Poll::Ready(res)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();

        if *this.tx_state == TXState::Broken {
            return Poll::Ready(Err(io::Error::broken_transport()));
        }

        let res = futures::ready!(this.poll_shutdown(cx));

        if res.is_err() {
            this.mark_broken();
        }

        Poll::Ready(res)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

impl<T> BaseIOProjection<'_, T>
where
    T: AsyncRead + AsyncWrite,
{
    /// Internal write operation wrapper.
    ///
    /// Precondition checks and state updates are handled by the caller.
    fn poll_write_with<F>(&mut self, cx: &mut Context<'_>, f: F) -> Poll<io::Result<usize>>
    where
        F: FnOnce(Pin<&mut T>, &mut Context<'_>) -> Poll<io::Result<usize>>,
    {
        debug_assert_eq!(*self.tx_state, TXState::Open);

        let inner = self.inner.as_mut();

        let Poll::Ready(res) = f(inner, cx) else {
            if self.tx_deadline.is_none() {
                self.set_tx_deadline(*self.write_timeout);
            }

            if self.poll_tx_deadline_ready(cx) {
                return Poll::Ready(Err(io::Error::write_timeout()));
            }

            return Poll::Pending;
        };

        self.clear_tx_deadline();

        Poll::Ready(res)
    }

    /// Internal flush operation implementation.
    ///
    /// Precondition checks and state updates are handled by the caller.
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        debug_assert!(matches!(*self.tx_state, TXState::Open | TXState::Closing));

        let inner = self.inner.as_mut();

        let Poll::Ready(res) = inner.poll_flush(cx) else {
            if self.tx_deadline.is_none() {
                self.set_tx_deadline(*self.write_timeout);
            }

            if self.poll_tx_deadline_ready(cx) {
                return Poll::Ready(Err(io::Error::write_timeout()));
            }

            return Poll::Pending;
        };

        // NOTE: It is unlikely that someone would call poll_flush() after
        //   initiating a shutdown but just in case, we clear the deadline
        //   here only if the TX state is still open. Otherwise, we could
        //   interfere with the shutdown deadline.
        if *self.tx_state == TXState::Open {
            self.clear_tx_deadline();
        }

        Poll::Ready(res)
    }

    /// Internal shutdown operation implementation.
    ///
    /// Precondition checks and error state handling is done by the caller.
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        debug_assert_ne!(*self.tx_state, TXState::Broken);

        // initiate the shutdown process if the TX channel is still open
        if *self.tx_state == TXState::Open {
            self.initiate_shutdown();
        }

        while *self.tx_state != TXState::ClosedAndDrained {
            // check if the close timeout has expired
            if self.poll_tx_deadline_ready(cx) {
                return Poll::Ready(Err(io::Error::close_timeout()));
            }

            // shutdown the underlying transport if it is still open
            if *self.tx_state == TXState::Closing {
                let inner = self.inner.as_mut();

                futures::ready!(inner.poll_shutdown(cx))?;

                *self.tx_state = TXState::Closed;
            }

            // and drain the RX channel until it is closed
            if *self.rx_state == RXState::Closed {
                *self.tx_state = TXState::ClosedAndDrained;
            } else if futures::ready!(self.poll_drain(cx))? == 0 {
                *self.rx_state = RXState::Closed;
                *self.tx_state = TXState::ClosedAndDrained;
            }
        }

        Poll::Ready(Ok(()))
    }

    /// Drain the RX channel and return the number of bytes drained.
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let mut buf = [MaybeUninit::<u8>::uninit(); 4096];

        let mut buf = ReadBuf::uninit(&mut buf);

        let inner = self.inner.as_mut();

        futures::ready!(inner.poll_read(cx, &mut buf))?;

        let filled = buf.filled();

        Poll::Ready(Ok(filled.len()))
    }
}

impl<T> BaseIOProjection<'_, T> {
    /// Set a TX operation deadline.
    fn set_tx_deadline(&mut self, after: Duration) {
        self.tx_deadline.set(Some(tokio::time::sleep(after)));
    }

    /// Clear the current TX operation deadline.
    fn clear_tx_deadline(&mut self) {
        self.tx_deadline.set(None);
    }

    /// Poll the current TX operation deadline.
    fn poll_tx_deadline_ready(&mut self, cx: &mut Context<'_>) -> bool {
        self.tx_deadline
            .as_mut()
            .as_pin_mut()
            .expect("deadline is not set")
            .poll(cx)
            .is_ready()
    }

    /// Initiate the shutdown procedure.
    fn initiate_shutdown(&mut self) {
        debug_assert_eq!(*self.tx_state, TXState::Open);

        *self.tx_state = TXState::Closing;

        if let Some(task) = self.reader.take() {
            task.wake();
        }

        self.set_tx_deadline(*self.close_timeout);
    }

    /// Mark the transport as broken.
    fn mark_broken(&mut self) {
        *self.tx_state = TXState::Broken;

        if let Some(task) = self.reader.take() {
            task.wake();
        }
    }
}

/// RX state.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum RXState {
    Open,
    Closed,
}

/// TX state.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TXState {
    Open,
    Closing,
    Closed,
    ClosedAndDrained,
    Broken,
}

/// Helper trait.
trait ErrorExt {
    /// Create a new write timeout error.
    fn write_timeout() -> Self;

    /// Create a new close timeout error.
    fn close_timeout() -> Self;

    /// Create a new closing transport error.
    fn closing_transport() -> Self;

    /// Create a new closed transport error.
    fn closed_transport() -> Self;

    /// Create a new broken transport error.
    fn broken_transport() -> Self;
}

impl ErrorExt for io::Error {
    fn write_timeout() -> Self {
        io::Error::new(io::ErrorKind::TimedOut, "write timeout")
    }

    fn close_timeout() -> Self {
        io::Error::new(io::ErrorKind::TimedOut, "close timeout")
    }

    fn closing_transport() -> Self {
        io::Error::new(io::ErrorKind::BrokenPipe, "closing transport")
    }

    fn closed_transport() -> Self {
        io::Error::new(io::ErrorKind::BrokenPipe, "closed transport")
    }

    fn broken_transport() -> Self {
        io::Error::new(io::ErrorKind::BrokenPipe, "broken transport")
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf},
        time::Instant,
    };

    use super::BaseIO;

    /// Duplex stream that panics if it is read again after reporting EOF.
    struct SingleEofStream {
        inner: DuplexStream,
        eof: bool,
    }

    impl AsyncRead for SingleEofStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            assert!(!self.eof, "read after EOF");

            let remaining_before = buf.remaining();

            futures::ready!(AsyncRead::poll_read(Pin::new(&mut self.inner), cx, buf))?;

            if remaining_before > 0 && buf.remaining() == remaining_before {
                self.eof = true;
            }

            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for SingleEofStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            AsyncWrite::poll_write(Pin::new(&mut self.inner), cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            AsyncWrite::poll_flush(Pin::new(&mut self.inner), cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            AsyncWrite::poll_shutdown(Pin::new(&mut self.inner), cx)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_write_timeout_breaks_reader() {
        let write_timeout = Duration::from_secs(10);
        let close_timeout = Duration::from_secs(10);

        // NOTE: The remote end is kept alive but it never reads anything.
        let (local, remote) = tokio::io::duplex(64);

        let io = BaseIO::new(local, write_timeout, close_timeout);

        let (mut rx, mut tx) = tokio::io::split(Box::pin(io));

        let reader = tokio::spawn(async move {
            let mut buf = [0u8; 16];

            rx.read(&mut buf).await
        });

        let start = Instant::now();

        let err = loop {
            if let Err(err) = tx.write(&[0u8; 64]).await {
                break err;
            }
        };

        assert_eq!(err.kind(), io::ErrorKind::TimedOut);

        assert!(start.elapsed() >= write_timeout);

        // the pending reader must be woken up with an error, not with EOF
        let err = reader.await.unwrap().unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);

        // ... and the transport cannot be closed gracefully anymore
        assert!(tx.shutdown().await.is_err());

        std::mem::drop(remote);
    }

    #[tokio::test(start_paused = true)]
    async fn test_shutdown_lingers_until_remote_eof() {
        let write_timeout = Duration::from_secs(10);
        let close_timeout = Duration::from_secs(10);

        let (local, mut remote) = tokio::io::duplex(1024);

        let io = BaseIO::new(local, write_timeout, close_timeout);

        let (mut rx, mut tx) = tokio::io::split(Box::pin(io));

        let reader = tokio::spawn(async move {
            let mut buf = [0u8; 16];

            rx.read(&mut buf).await
        });

        // let the reader block on the empty transport
        tokio::time::sleep(Duration::from_millis(1)).await;

        tx.write_all(b"hello").await.unwrap();

        let shutdown = tokio::spawn(async move { tx.shutdown().await });

        // the pending reader gets EOF as soon as the shutdown starts
        assert_eq!(reader.await.unwrap().unwrap(), 0);

        // the remote peer receives all data followed by EOF
        let mut received = Vec::new();

        remote.read_to_end(&mut received).await.unwrap();

        assert_eq!(received, b"hello");

        // data sent by the remote peer after our shutdown is drained (the
        // remote write would block forever otherwise because the data does
        // not fit into the transport buffer)
        remote.write_all(&[0u8; 4096]).await.unwrap();

        // the shutdown must not complete until the remote peer closes the
        // connection as well
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(!shutdown.is_finished());

        std::mem::drop(remote);

        shutdown.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_close_timeout() {
        let write_timeout = Duration::from_secs(10);
        let close_timeout = Duration::from_secs(5);

        let (local, mut remote) = tokio::io::duplex(1024);

        let io = BaseIO::new(local, write_timeout, close_timeout);

        let mut io = Box::pin(io);

        let start = Instant::now();

        // the remote peer never closes the connection
        let err = io.shutdown().await.unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::TimedOut);

        assert!(start.elapsed() >= close_timeout);

        let mut buf = [0u8; 16];

        // the remote peer still got our EOF
        assert_eq!(remote.read(&mut buf).await.unwrap(), 0);

        // ... but we cannot read from a transport that failed to close
        let err = io.read(&mut buf).await.unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test(start_paused = true)]
    async fn test_shutdown_after_remote_eof_skips_drain() {
        let write_timeout = Duration::from_secs(10);
        let close_timeout = Duration::from_secs(10);

        let (local, mut remote) = tokio::io::duplex(1024);

        let local = SingleEofStream {
            inner: local,
            eof: false,
        };

        let io = BaseIO::new(local, write_timeout, close_timeout);

        let mut io = Box::pin(io);

        remote.shutdown().await.unwrap();

        let mut buf = [0u8; 16];

        assert_eq!(io.read(&mut buf).await.unwrap(), 0);

        // NOTE: The underlying stream panics if it is read again after EOF.
        let start = Instant::now();

        io.shutdown().await.unwrap();

        assert_eq!(start.elapsed(), Duration::ZERO);

        let mut received = Vec::new();

        remote.read_to_end(&mut received).await.unwrap();

        assert!(received.is_empty());
    }
}
