use std::{
    collections::VecDeque,
    task::{Context, Poll, Waker},
};

use bytes::Bytes;

use crate::v3::{
    error::Error,
    msg::{
        error::ErrorMessage,
        raw::{RawDataAckMessage, RawDataMessage},
    },
    service::{error::ServiceProtocolError, msg::ServiceConnectionMessage},
};

/// Service connection context.
pub struct ServiceConnectionContext {
    incoming_items: VecDeque<Result<Bytes, Error>>,
    outgoing_messages: VecDeque<ServiceConnectionMessage>,
    outgoing_data: Bytes,
    incoming_closed: bool,
    outgoing_closed: bool,

    rx_capacity: usize,
    rx_unacknowledged: usize,
    rx_acknowledge: usize,

    tx_capacity: usize,
    max_tx_payload_size: usize,

    foreground_rx_task: Option<Waker>,
    foreground_tx_task: Option<Waker>,
    background_tx_task: Option<Waker>,
}

impl ServiceConnectionContext {
    /// Create a new service connection context.
    pub fn new(rx_capacity: usize, tx_capacity: usize, max_tx_payload_size: usize) -> Self {
        Self {
            incoming_items: VecDeque::new(),
            outgoing_messages: VecDeque::new(),
            outgoing_data: Bytes::new(),
            incoming_closed: false,
            outgoing_closed: false,

            rx_capacity,
            rx_unacknowledged: 0,
            rx_acknowledge: 0,

            tx_capacity,
            max_tx_payload_size,

            foreground_rx_task: None,
            foreground_tx_task: None,
            background_tx_task: None,
        }
    }

    /// Check if the incoming side is closed.
    pub fn incoming_closed(&self) -> bool {
        self.incoming_closed
    }

    /// Process a given incoming item.
    pub fn process_incoming_item(
        &mut self,
        item: Result<ServiceConnectionMessage, ServiceProtocolError>,
    ) {
        if self.incoming_closed {
            return;
        }

        let res = match item {
            Ok(msg) => self.process_incoming_message(msg),
            Err(err) => Err(err),
        };

        if let Err(err) = res {
            self.process_incoming_error(err);
        }
    }

    /// Process a given incoming message.
    fn process_incoming_message(
        &mut self,
        msg: ServiceConnectionMessage,
    ) -> Result<(), ServiceProtocolError> {
        match msg {
            ServiceConnectionMessage::Data(msg) => self.process_incoming_data_message(msg),
            ServiceConnectionMessage::DataAck(msg) => self.process_incoming_ack_message(msg),
            ServiceConnectionMessage::Error(msg) => self.process_incoming_error_message(msg),
        }
    }

    /// Process a given incoming error.
    fn process_incoming_error(&mut self, err: ServiceProtocolError) {
        if let Some(msg) = err.to_error_message() {
            self.send_outgoing_message(msg);
        }

        self.incoming_items.push_back(Err(err.into()));

        self.notify_foreground_rx_task();

        self.close_incoming();
    }

    /// Process an incoming error message.
    fn process_incoming_error_message(
        &mut self,
        msg: ErrorMessage,
    ) -> Result<(), ServiceProtocolError> {
        let msg = format!("received error message: {msg}");

        let err = Error::from_msg(msg);

        Err(ServiceProtocolError::Other(err))
    }

    /// Process an incoming ACK message.
    fn process_incoming_ack_message(
        &mut self,
        msg: RawDataAckMessage,
    ) -> Result<(), ServiceProtocolError> {
        let ack = msg.length();

        self.tx_capacity = self.tx_capacity.saturating_add(ack as usize);

        self.notify_background_tx_task();

        Ok(())
    }

    /// Process an incoming data message.
    fn process_incoming_data_message(
        &mut self,
        msg: RawDataMessage,
    ) -> Result<(), ServiceProtocolError> {
        let mut data = msg.into_data();

        let take = self
            .rx_capacity
            .saturating_sub(self.rx_unacknowledged)
            .min(data.len());

        let chunk = data.split_to(take);

        self.rx_unacknowledged += chunk.len();

        self.incoming_items.push_back(Ok(chunk));

        self.notify_foreground_rx_task();

        if data.is_empty() {
            Ok(())
        } else {
            Err(ServiceProtocolError::ChannelCapacityExceeded)
        }
    }

    /// Close the incoming side.
    pub fn close_incoming(&mut self) {
        if self.incoming_closed {
            return;
        }

        self.incoming_closed = true;

        self.notify_foreground_rx_task();
        self.notify_background_tx_task();

        // NOTE: The contract is to close the outgoing channel when the
        //   incoming channel gets closed. Any remaining outgoing data will
        //   not be sent and the foreground TX task will get an error on
        //   flush.
        self.close_outgoing();
    }

    /// Close the outgoing side.
    pub fn close_outgoing(&mut self) {
        if self.outgoing_closed {
            return;
        }

        self.outgoing_closed = true;

        self.notify_foreground_tx_task();
        self.notify_background_tx_task();
    }

    /// Poll incoming data.
    pub fn poll_incoming(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes, Error>>> {
        if let Some(next) = self.incoming_items.pop_front() {
            if let Ok(chunk) = next.as_ref() {
                // NOTE: We acknowledge received data only after it has
                //   been consumed by the stream consumer. This way we can
                //   stop the remote peer from sending more data if the
                //   consumer is too slow.
                self.rx_acknowledge += chunk.len();
            }

            self.notify_background_tx_task();

            Poll::Ready(Some(next))
        } else if self.incoming_closed {
            Poll::Ready(None)
        } else {
            let task = cx.waker();

            self.foreground_rx_task = Some(task.clone());

            Poll::Pending
        }
    }

    /// Poll outgoing messages.
    pub fn poll_outgoing(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ServiceConnectionMessage>> {
        if let Some(msg) = self.outgoing_messages.pop_front() {
            Poll::Ready(Some(msg))
        } else if self.outgoing_closed {
            // NOTE: The `self.outgoing_closed` means that either the incoming
            //   channel got closed by the remote peer and we're obligated to
            //   close the outgoing channel as well or the foreground TX task
            //   wishes to close the outgoing channel. Note that sending ACK
            //   messages for any unacknowledged data is not necessary. If the
            //   closure was initiated by the remote peer, our ACKs would
            //   unchoke it but it wouldn't be able to send any data because it
            //   already closed its outgoing channel. If the closure was
            //   initiated locally, sending ACKs to the remote peer would
            //   unchoke it and allow it to send more data preventing us from
            //   closing the connection.
            Poll::Ready(None)
        } else if self.rx_acknowledge > 0 {
            let ack = RawDataAckMessage::new(self.rx_acknowledge as u32);

            self.rx_unacknowledged -= self.rx_acknowledge;
            self.rx_acknowledge = 0;

            Poll::Ready(Some(ack.into()))
        } else if self.tx_capacity > 0 && !self.outgoing_data.is_empty() {
            let take = self
                .outgoing_data
                .len()
                .min(self.max_tx_payload_size)
                .min(self.tx_capacity);

            self.tx_capacity -= take;

            let chunk = self.outgoing_data.split_to(take);

            if self.outgoing_data.is_empty() {
                self.notify_foreground_tx_task();
            }

            let msg = RawDataMessage::new(chunk);

            Poll::Ready(Some(msg.into()))
        } else {
            let task = cx.waker();

            self.background_tx_task = Some(task.clone());

            Poll::Pending
        }
    }

    /// Poll readiness for sending outgoing data.
    pub fn poll_ready_outgoing(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.outgoing_closed {
            Poll::Ready(Err(Error::from_static_msg("connection closed")))
        } else if self.outgoing_data.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            let task = cx.waker();

            self.foreground_tx_task = Some(task.clone());

            Poll::Pending
        }
    }

    /// Send outgoing data.
    pub fn start_send_outgoing(&mut self, chunk: Bytes) -> Result<(), Error> {
        if self.outgoing_closed {
            Err(Error::from_static_msg("connection closed"))
        } else if self.outgoing_data.is_empty() {
            self.outgoing_data = chunk;

            self.notify_background_tx_task();

            Ok(())
        } else {
            panic!("not ready");
        }
    }

    /// Flush outgoing data.
    pub fn poll_flush_outgoing(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.outgoing_data.is_empty() {
            Poll::Ready(Ok(()))
        } else if self.outgoing_closed {
            Poll::Ready(Err(Error::from_static_msg("connection closed")))
        } else {
            let task = cx.waker();

            self.foreground_tx_task = Some(task.clone());

            Poll::Pending
        }
    }

    /// Flush the outgoing data and close the outgoing side of the connection.
    pub fn poll_close_outgoing(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        futures::ready!(self.poll_flush_outgoing(cx))?;

        self.close_outgoing();

        Poll::Ready(Ok(()))
    }

    /// Send a given outgoing message.
    fn send_outgoing_message<T>(&mut self, msg: T)
    where
        T: Into<ServiceConnectionMessage>,
    {
        self.outgoing_messages.push_back(msg.into());

        self.notify_background_tx_task();
    }

    /// Wake up the foreground RX task.
    fn notify_foreground_rx_task(&mut self) {
        if let Some(task) = self.foreground_rx_task.take() {
            task.wake();
        }
    }

    /// Wake up the foreground TX task.
    fn notify_foreground_tx_task(&mut self) {
        if let Some(task) = self.foreground_tx_task.take() {
            task.wake();
        }
    }

    /// Wake up the background TX task.
    fn notify_background_tx_task(&mut self) {
        if let Some(task) = self.background_tx_task.take() {
            task.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll};

    use bytes::Bytes;

    use super::ServiceConnectionContext;

    use crate::v3::{msg::raw::RawDataMessage, service::msg::ServiceConnectionMessage};

    #[test]
    fn test_remote_close_fails_pending_flush() {
        let waker = futures::task::noop_waker();

        let mut cx = Context::from_waker(&waker);

        let mut context = ServiceConnectionContext::new(1024, 1024, 1024);

        context
            .start_send_outgoing(Bytes::from(vec![0u8; 2048]))
            .unwrap();

        // the first chunk exhausts the TX capacity
        let Poll::Ready(Some(ServiceConnectionMessage::Data(msg))) = context.poll_outgoing(&mut cx)
        else {
            panic!("expected a data message");
        };

        assert_eq!(msg.into_data().len(), 1024);

        assert!(context.poll_outgoing(&mut cx).is_pending());
        assert!(context.poll_flush_outgoing(&mut cx).is_pending());

        // the remote peer closes the connection, so the rest of the data can
        // never be sent
        context.close_incoming();

        // NOTE: The remaining data must not be reported as flushed.
        assert!(matches!(
            context.poll_flush_outgoing(&mut cx),
            Poll::Ready(Err(_))
        ));

        assert!(matches!(context.poll_outgoing(&mut cx), Poll::Ready(None)));

        assert!(matches!(
            context.poll_ready_outgoing(&mut cx),
            Poll::Ready(Err(_))
        ));
    }

    #[test]
    fn test_no_ack_after_local_close() {
        let waker = futures::task::noop_waker();

        let mut cx = Context::from_waker(&waker);

        let mut context = ServiceConnectionContext::new(1024, 1024, 1024);

        let data = RawDataMessage::new(Bytes::from(vec![0u8; 512]));

        context.process_incoming_item(Ok(data.into()));

        // consuming the data would normally trigger an ACK
        assert!(matches!(
            context.poll_incoming(&mut cx),
            Poll::Ready(Some(Ok(_)))
        ));

        assert!(matches!(
            context.poll_close_outgoing(&mut cx),
            Poll::Ready(Ok(()))
        ));

        // ... but the ACK would allow the remote peer to send more data while
        // we're trying to close the connection
        assert!(matches!(context.poll_outgoing(&mut cx), Poll::Ready(None)));

        // data arriving after the local close can still be read
        let data = RawDataMessage::new(Bytes::from(vec![0u8; 256]));

        context.process_incoming_item(Ok(data.into()));

        let Poll::Ready(Some(Ok(chunk))) = context.poll_incoming(&mut cx) else {
            panic!("expected a data chunk");
        };

        assert_eq!(chunk.len(), 256);
    }
}
