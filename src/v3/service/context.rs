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
    incoming_eof: bool,
    outgoing_eof: bool,

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
            incoming_eof: false,
            outgoing_eof: false,

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
    pub fn incoming_eof(&self) -> bool {
        self.incoming_eof
    }

    /// Process a given incoming item.
    pub fn process_incoming_item(
        &mut self,
        item: Result<ServiceConnectionMessage, ServiceProtocolError>,
    ) {
        if self.incoming_eof {
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
        if self.incoming_eof {
            return;
        }

        self.incoming_eof = true;

        self.notify_foreground_rx_task();
        self.notify_background_tx_task();
    }

    /// Close the outgoing side.
    pub fn close_outgoing(&mut self) {
        if self.outgoing_eof {
            return;
        }

        self.outgoing_eof = true;

        self.notify_foreground_tx_task();
        self.notify_background_tx_task();
    }

    /// Terminate the connection.
    pub fn terminate(&mut self) {
        self.incoming_items.clear();

        self.notify_background_tx_task();

        self.close_incoming();
        self.close_outgoing();
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
        } else if self.incoming_eof {
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
        } else if self.incoming_eof && self.incoming_items.is_empty() {
            // NOTE: The `self.incoming_eof` means that there will be no more
            //   incoming data ACK messages, so even if there is still outgoing
            //   data, we will never be able to send it. And if there are also
            //   no more incoming items to deliver, there will be no more
            //   outgoing data ACK messages either, so we can just terminate
            //   the outgoing side of the connection.

            // NOTE: The `self.outgoing_eof` value is irrelevant here, because
            //   even if the outgoing channel is closed, we still need to be
            //   able to send any potential outgoing data ACK messages or error
            //   messages.
            Poll::Ready(None)
        } else {
            let task = cx.waker();

            self.background_tx_task = Some(task.clone());

            Poll::Pending
        }
    }

    /// Poll readiness for sending outgoing data.
    pub fn poll_ready_outgoing(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.outgoing_eof {
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
        if self.outgoing_eof {
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
        } else if self.outgoing_eof {
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
