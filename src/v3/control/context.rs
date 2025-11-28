use std::{
    collections::{HashMap, VecDeque},
    task::{Context, Poll, Waker},
};

use futures::channel::oneshot;

use crate::v3::{
    control::{error::ControlProtocolError, msg::ControlProtocolMessage},
    msg::{
        error::ErrorMessage,
        json::{
            JsonRpcMethod, JsonRpcNotification, JsonRpcParams, JsonRpcRequest, JsonRpcResponse,
        },
    },
};

/// Control protocol connection context.
pub struct ConnectionContext {
    pending_requests: HashMap<u64, OutgoingRequestHandle>,
    outgoing_requests: VecDeque<OutgoingRequest>,
    outgoing_responses: VecDeque<JsonRpcResponse>,
    outgoing_notifications: VecDeque<OutgoingNotification>,
    outgoing_messages: VecDeque<ControlProtocolMessage>,
    outgoing_message_consumer: Option<Waker>,
    next_outgoing_request_id: u64,
    max_remote_concurrent_requests: usize,
    max_local_concurrent_requests: usize,
    current_local_concurrent_requests: usize,
    closed: bool,
}

impl ConnectionContext {
    /// Create a new connection context.
    pub fn new(
        max_local_concurrent_requests: usize,
        max_remote_concurrent_requests: usize,
    ) -> Self {
        Self {
            pending_requests: HashMap::new(),
            outgoing_requests: VecDeque::new(),
            outgoing_responses: VecDeque::new(),
            outgoing_notifications: VecDeque::new(),
            outgoing_messages: VecDeque::with_capacity(3),
            outgoing_message_consumer: None,
            next_outgoing_request_id: 0,
            max_remote_concurrent_requests,
            max_local_concurrent_requests,
            current_local_concurrent_requests: 0,
            closed: false,
        }
    }

    /// Accept an incoming request.
    ///
    /// This is used to track the number of concurrent incoming requests.
    pub fn accept_incoming_request(&mut self) -> Result<(), ControlProtocolError> {
        if self.current_local_concurrent_requests >= self.max_local_concurrent_requests {
            return Err(ControlProtocolError::TooManyConcurrentRequests);
        }

        self.current_local_concurrent_requests += 1;

        Ok(())
    }

    /// Push a given response to the outgoing response queue.
    ///
    /// A call to this method must be paired with a prior call to
    /// `accept_incoming_request` to ensure proper tracking of concurrent
    /// incoming requests.
    pub fn push_outgoing_response(&mut self, response: JsonRpcResponse) {
        self.current_local_concurrent_requests -= 1;

        if self.closed {
            return;
        }

        self.outgoing_responses.push_back(response);

        self.notify_outgoing_message_consumer();
    }

    /// Push a given notification to the outgoing notification queue.
    pub fn push_outgoing_notification(&mut self, notification: OutgoingNotification) {
        if self.closed {
            return;
        }

        self.outgoing_notifications.push_back(notification);

        self.notify_outgoing_message_consumer();
    }

    /// Push a given request to the outgoing request queue.
    pub fn push_outgoing_request(&mut self, request: OutgoingRequest) {
        if self.closed {
            return;
        }

        self.outgoing_requests.push_back(request);

        self.notify_outgoing_message_consumer();
    }

    /// Poll the next outgoing message.
    pub fn poll_outgoing_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ControlProtocolMessage>> {
        if !self.closed && self.outgoing_messages.is_empty() {
            // take exactly one request (if possible)
            if self.pending_requests.len() < self.max_remote_concurrent_requests
                && let Some(r) = self.outgoing_requests.pop_front()
            {
                let id = self.next_outgoing_request_id;

                self.next_outgoing_request_id = self.next_outgoing_request_id.wrapping_add(1);

                let (request, handle) = r.into_json_rpc_request(id);

                self.pending_requests.insert(id, handle);
                self.outgoing_messages.push_back(request.into());
            }

            // ... one notification
            if let Some(n) = self.outgoing_notifications.pop_front() {
                let (notification, handle) = n.into_json_rpc_notification();

                self.outgoing_messages.push_back(notification.into());

                // XXX: We mark the notification as sent even before it is
                //   actually sent over the network. It isn't ideal, we might
                //   improve this later if needed.
                handle.resolve();
            }

            // ... and one response in order to ensure fair processing
            if let Some(r) = self.outgoing_responses.pop_front() {
                self.outgoing_messages.push_back(r.into());
            }
        }

        if let Some(msg) = self.outgoing_messages.pop_front() {
            return Poll::Ready(Some(msg));
        } else if self.closed {
            return Poll::Ready(None);
        }

        let task = cx.waker();

        self.outgoing_message_consumer = Some(task.clone());

        Poll::Pending
    }

    /// Process an incoming response.
    pub fn process_incoming_response(
        &mut self,
        response: JsonRpcResponse,
    ) -> Result<(), ControlProtocolError> {
        let id = response.id();

        if let Some(handle) = self.pending_requests.remove(&id) {
            handle.resolve(response);

            // Notify the outgoing message consumer in case it is waiting for
            // an available outgoing request slot.
            self.notify_outgoing_message_consumer();

            return Ok(());
        }

        Err(ControlProtocolError::UnexpectedResponseId(id))
    }

    /// Close the context.
    ///
    /// This will clear all outgoing message queues and pending requests and
    /// the outgoing message consumer will be notified that there will be no
    /// more messages.
    pub fn close(&mut self) {
        if self.closed {
            return;
        }

        self.closed = true;

        // Once the connection is closed, we can't send any more messages and
        // we won't receive anything either, so we might as well just drop all
        // outgoing and pending requests, responses, notifications and the
        // outgoing message queue.
        self.pending_requests.clear();
        self.outgoing_requests.clear();
        self.outgoing_responses.clear();
        self.outgoing_notifications.clear();
        self.outgoing_messages.clear();

        // We need to wake up the outgoing message consumer so that it can
        // notice that the connection is closed.
        self.notify_outgoing_message_consumer();
    }

    /// Send a given error message and close the context.
    pub fn send_error_message(&mut self, error: ErrorMessage) {
        if self.closed {
            return;
        }

        self.closed = true;

        // This is a connection-level error, so we need to stop sending any
        // messages and send only the error message. The connection will be
        // closed after that.
        self.pending_requests.clear();
        self.outgoing_requests.clear();
        self.outgoing_responses.clear();
        self.outgoing_notifications.clear();
        self.outgoing_messages.clear();

        self.outgoing_messages.push_back(error.into());

        self.notify_outgoing_message_consumer();
    }

    /// Notify the outgoing message consumer that it should poll for outgoing
    /// messages again.
    fn notify_outgoing_message_consumer(&mut self) {
        if let Some(task) = self.outgoing_message_consumer.take() {
            task.wake();
        }
    }
}

/// Future incoming response.
pub type IncomingResponseFuture = oneshot::Receiver<JsonRpcResponse>;

/// Outgoing request.
pub struct OutgoingRequest {
    method: JsonRpcMethod,
    params: JsonRpcParams,
    handle: OutgoingRequestHandle,
}

impl OutgoingRequest {
    /// Create a new outgoing request.
    pub fn new(method: JsonRpcMethod, params: JsonRpcParams) -> (Self, IncomingResponseFuture) {
        let (tx, rx) = oneshot::channel();

        let handle = OutgoingRequestHandle { inner: tx };

        let request = Self {
            method,
            params,
            handle,
        };

        (request, rx)
    }

    /// Convert the request into a JSON-RPC request and its handle.
    fn into_json_rpc_request(self, id: u64) -> (JsonRpcRequest, OutgoingRequestHandle) {
        let request = JsonRpcRequest::new(id, self.method, self.params);

        (request, self.handle)
    }
}

/// Outgoing request handle that can be used to resolve the request.
struct OutgoingRequestHandle {
    inner: oneshot::Sender<JsonRpcResponse>,
}

impl OutgoingRequestHandle {
    /// Resolve the request with a given incoming response.
    fn resolve(self, response: JsonRpcResponse) {
        let _ = self.inner.send(response);
    }
}

/// Future notification sent confirmation.
pub type NotificationSentFuture = oneshot::Receiver<()>;

/// Outgoing notification.
pub struct OutgoingNotification {
    method: JsonRpcMethod,
    params: JsonRpcParams,
    handle: OutgoingNotificationHandle,
}

impl OutgoingNotification {
    /// Create a new outgoing notification.
    pub fn new(method: JsonRpcMethod, params: JsonRpcParams) -> (Self, NotificationSentFuture) {
        let (tx, rx) = oneshot::channel();

        let handle = OutgoingNotificationHandle { inner: tx };

        let request = Self {
            method,
            params,
            handle,
        };

        (request, rx)
    }

    /// Convert the notification into a JSON-RPC notification and its handle.
    fn into_json_rpc_notification(self) -> (JsonRpcNotification, OutgoingNotificationHandle) {
        let notification = JsonRpcNotification::new(self.method, self.params);

        (notification, self.handle)
    }
}

/// Outgoing notification handle that can be used to confirm sending.
struct OutgoingNotificationHandle {
    inner: oneshot::Sender<()>,
}

impl OutgoingNotificationHandle {
    /// Confirm that the notification was sent.
    fn resolve(self) {
        let _ = self.inner.send(());
    }
}
