use std::time::{Duration, Instant};

use futures::{
    SinkExt, StreamExt,
    channel::{mpsc, oneshot},
    future::{AbortHandle, AbortRegistration, Abortable},
};

/// PING-PONG error.
pub enum PingPongError {
    UnexpectedPongId(u16),
    Timeout,
}

/// Incoming PONG sender.
pub type IncomingPongSender = mpsc::Sender<u16>;

/// Incoming PONG receiver.
pub type IncomingPongReceiver = mpsc::Receiver<u16>;

/// Outgoing PING sender.
pub type OutgoingPingSender = mpsc::Sender<u16>;

/// Outgoing PING receiver.
pub type OutgoingPingReceiver = mpsc::Receiver<u16>;

/// PING-PONG error sender.
pub type PingPongErrorSender = oneshot::Sender<PingPongError>;

/// PING-PONG error receiver.
pub type PingPongErrorReceiver = oneshot::Receiver<PingPongError>;

/// PING-PONG handler.
///
/// The PING-PONG handler should be spawned as a separate task after the
/// initial handshake is complete. It periodically sends PING messages to the
/// remote peer and waits for corresponding PONG responses. If a PONG is not
/// received within the specified timeout, or if an unexpected PONG ID is
/// received, it reports an error through the provided channel.
///
/// The task will terminate gracefully when the channel is closed or when the
/// abort handle is triggered. The returned error receiver will emit a
/// `Cancelled` error if the task is dropped or terminated.
pub struct PingPongHandler {
    abort: AbortRegistration,
    inner: InternalPingPongHandler,
}

impl PingPongHandler {
    /// Create a new PING-PONG handler.
    pub(super) fn new(
        ping_tx: OutgoingPingSender,
        pong_rx: IncomingPongReceiver,
    ) -> (Self, PingPongErrorReceiver, AbortHandle) {
        let (error_tx, error_rx) = oneshot::channel();

        let inner = InternalPingPongHandler {
            pong_rx,
            ping_tx,
            error_tx,
        };

        let (abort_handle, abort_registration) = AbortHandle::new_pair();

        let handler = Self {
            abort: abort_registration,
            inner,
        };

        (handler, error_rx, abort_handle)
    }

    /// Run the PING-PONG handler.
    pub async fn run(self, ping_interval: Duration, pong_timeout: Duration) {
        let run = self.inner.run(ping_interval, pong_timeout);

        let _ = Abortable::new(run, self.abort).await;
    }
}

/// Internal PING-PONG handler.
struct InternalPingPongHandler {
    pong_rx: IncomingPongReceiver,
    ping_tx: OutgoingPingSender,
    error_tx: PingPongErrorSender,
}

impl InternalPingPongHandler {
    /// Run the PING-PONG handler.
    async fn run(mut self, ping_interval: Duration, pong_timeout: Duration) {
        let now = Instant::now();

        let mut next_ping_at = now + ping_interval;

        let mut next_ping_id: u16 = 0;

        let err = loop {
            let ping_id = next_ping_id;

            next_ping_id = next_ping_id.wrapping_add(1);

            tokio::time::sleep_until(next_ping_at.into()).await;

            let send = self.ping_tx.send(ping_id);

            if send.await.is_err() {
                return;
            }

            let next = tokio::time::timeout(pong_timeout, self.pong_rx.next());

            match next.await {
                Ok(Some(pong_id)) if ping_id == pong_id => (),
                Ok(Some(pong_id)) => break PingPongError::UnexpectedPongId(pong_id),
                Ok(None) => return,
                Err(_) => break PingPongError::Timeout,
            }

            let now = Instant::now();

            next_ping_at = now.max(next_ping_at + ping_interval);
        };

        let _ = self.error_tx.send(err);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{SinkExt, StreamExt, channel::mpsc};

    use super::{PingPongError, PingPongHandler};

    #[tokio::test]
    async fn test_ping_pong_handler_abort() {
        let (ping_tx, ping_rx) = mpsc::channel::<u16>(10);
        let (pong_tx, pong_rx) = mpsc::channel::<u16>(10);

        let (handler, error_rx, abort_handle) = PingPongHandler::new(ping_tx, pong_rx);

        let ping_interval = Duration::from_secs(2);
        let pong_timeout = Duration::from_secs(1);

        let task = tokio::spawn(handler.run(ping_interval, pong_timeout));

        abort_handle.abort();

        let abort_timeout = Duration::from_millis(500);

        tokio::time::timeout(abort_timeout, task)
            .await
            .expect("ping pong handler did not abort in time")
            .expect("ping pong handler panicked");

        std::mem::drop((ping_rx, pong_tx, error_rx));
    }

    #[tokio::test]
    async fn test_ping_pong_handler_timeout() {
        let (ping_tx, mut ping_rx) = mpsc::channel::<u16>(10);
        let (mut pong_tx, pong_rx) = mpsc::channel::<u16>(10);

        let (handler, error_rx, abort_handle) = PingPongHandler::new(ping_tx, pong_rx);

        let ping_interval = Duration::from_millis(100);
        let pong_timeout = Duration::from_millis(100);

        tokio::spawn(async move {
            while let Some(ping_id) = ping_rx.next().await {
                // send the first pong immediately
                let _ = pong_tx.send(ping_id).await;

                // ... and then wait for some time to trigger the timeout
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        tokio::spawn(handler.run(ping_interval, pong_timeout));

        let error_timeout = Duration::from_millis(500);

        let err = tokio::time::timeout(error_timeout, error_rx)
            .await
            .expect("timeout waiting for ping pong error")
            .expect("ping pong task terminated");

        assert!(matches!(err, PingPongError::Timeout));

        abort_handle.abort();
    }

    #[tokio::test]
    async fn test_ping_pong_handler_unexpected_pong_id() {
        let (ping_tx, mut ping_rx) = mpsc::channel::<u16>(10);
        let (mut pong_tx, pong_rx) = mpsc::channel::<u16>(10);

        let (handler, error_rx, abort_handle) = PingPongHandler::new(ping_tx, pong_rx);

        let ping_interval = Duration::from_millis(100);
        let pong_timeout = Duration::from_millis(100);

        tokio::spawn(async move {
            while let Some(ping_id) = ping_rx.next().await {
                let _ = pong_tx.send(ping_id.wrapping_add(1)).await;
            }
        });

        tokio::spawn(handler.run(ping_interval, pong_timeout));

        let error_timeout = Duration::from_millis(500);

        let err = tokio::time::timeout(error_timeout, error_rx)
            .await
            .expect("timeout waiting for ping pong error")
            .expect("ping pong task terminated");

        assert!(matches!(err, PingPongError::UnexpectedPongId(_)));

        abort_handle.abort();
    }

    #[tokio::test]
    async fn test_ping_pong_handler_drop() {
        let (ping_tx, ping_rx) = mpsc::channel::<u16>(10);
        let (pong_tx, pong_rx) = mpsc::channel::<u16>(10);

        let (handler, error_rx, abort_handle) = PingPongHandler::new(ping_tx, pong_rx);

        std::mem::drop(handler);

        assert!(error_rx.await.is_err());

        std::mem::drop((ping_rx, pong_tx, abort_handle));
    }
}
