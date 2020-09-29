use std::{fmt, sync::Arc, time::Instant};

use serde::Deserialize;
use tokio::sync::broadcast;

use crate::{
    communication::{Pusher, SendEndpoint},
    dataflow::{Data, Message, Timestamp},
    deadlines::{Notification, NotificationType, Notifier},
};

use super::{errors::WriteStreamError, StreamId, WriteStreamT};

// TODO (Sukrit) :: This example needs to be fixed after we enable attaching WriteStreams to
// callbacks for normal read streams.
/// A [`WriteStream`] allows operators to send data to other operators.
///
/// An [`Operator`](crate::dataflow::operator::Operator) creates and returns [`WriteStream`] s in
/// its `connect` function. Operators receive the returned [`WriteStream`] s in their `new`
/// function, which can be attached to registered callbacks in order to send messages on the
/// streams.
///
/// A driver receives a set of [`ReadStream`] s corresponding to the [`WriteStream`] s returned by
/// an operator's `connect` function. In order to send data from the driver to an operator, the
/// driver can instantiate an [`IngestStream`] and pass it to the operator's [`connect`] function.
///
/// # Example
/// The following example shows an [`Operator`](crate::dataflow::operator::Operator) that takes a
/// single [`ReadStream`] and maps the received value to its square and send that on a
/// [`WriteStream`]:
/// ```
/// use erdos::dataflow::message::Message;
/// use erdos::dataflow::{
///     stream::WriteStreamT, Operator, ReadStream, Timestamp, WriteStream, OperatorConfig
/// };
/// pub struct SquareOperator {}
///
/// impl SquareOperator {
///     pub fn new(
///         config: OperatorConfig<()>,
///         input_stream: ReadStream<u32>,
///         write_stream: WriteStream<u64>,
///     ) -> Self {
///         let stateful_read_stream = input_stream.add_state(write_stream);
///         // Request a callback upon receipt of every message.
///         stateful_read_stream.add_callback(
///             move |t: &Timestamp, msg: &u32, stream: &mut WriteStream<u64>| {
///                 Self::on_callback(t, msg, stream);
///             },
///         );
///         Self {}
///     }
///
///     pub fn connect(input_stream: ReadStream<u32>) -> WriteStream<u64> {
///         WriteStream::new()
///     }
///
///     fn on_callback(t: &Timestamp, msg: &u32, write_stream: &mut WriteStream<u64>) {
///         write_stream.send(Message::new_message(t.clone(), (msg * msg) as u64));
///     }
/// }
///
/// impl Operator for SquareOperator {}
/// ```

#[derive(Clone)]
pub struct WriteStream<D: Data> {
    /// The unique ID of the stream (automatically generated by the constructor)
    id: StreamId,
    /// The name of the stream (String representation of the ID, if no name provided)
    name: String,
    /// Sends message to other operators.
    pusher: Option<Pusher<Arc<Message<D>>>>,
    /// Current low watermark.
    low_watermark: Timestamp,
    /// Whether the stream is closed.
    stream_closed: bool,
    /// Broadcasts notifications upon message receipt.
    /// TODO: reimplement this with unbounded channels.
    /// TODO: currently only works with callbacks (not run).
    notification_tx: broadcast::Sender<Notification>,
}

impl<D: Data> WriteStream<D> {
    /// Returns a new instance of the [`WriteStream`].
    pub fn new() -> Self {
        let id = StreamId::new_deterministic();
        WriteStream::new_internal(id, id.to_string())
    }

    /// Returns a new instance of the [`WriteStream`].
    ///
    /// # Arguments
    /// * `name` - The name to be given to the stream.
    pub fn new_with_name(name: &str) -> Self {
        WriteStream::new_internal(StreamId::new_deterministic(), name.to_string())
    }

    /// Returns a new instance of the [`WriteStream`].
    ///
    /// # Arguments
    /// * `id` - The ID of the stream.
    pub fn new_with_id(id: StreamId) -> Self {
        WriteStream::new_internal(id, id.to_string())
    }

    /// Creates the [`WriteStream`] to be used to send messages to the dataflow.
    fn new_internal(id: StreamId, name: String) -> Self {
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Initializing a WriteStream {} with the ID: {}",
            name,
            id
        );
        Self {
            id,
            name,
            pusher: Some(Pusher::new()),
            low_watermark: Timestamp::new(vec![0]),
            stream_closed: false,
            notification_tx: broadcast::channel(128).0,
        }
    }

    pub fn from_endpoints(endpoints: Vec<SendEndpoint<Arc<Message<D>>>>, id: StreamId) -> Self {
        let mut stream = Self::new_with_id(id);
        for endpoint in endpoints {
            stream.add_endpoint(endpoint);
        }
        stream
    }

    /// Get the ID given to the stream by the constructor
    pub fn get_id(&self) -> StreamId {
        self.id
    }

    /// Get the name of the stream.
    /// Returns a [`str`] version of the ID if the stream was not constructed with
    /// [`new_with_name`](IngestStream::new_with_name).
    pub fn get_name(&self) -> &str {
        &self.name[..]
    }

    /// Returns `true` if a top watermark message was received or the [`IngestStream`] failed to
    /// set up.
    pub fn is_closed(&self) -> bool {
        self.stream_closed
    }

    fn add_endpoint(&mut self, endpoint: SendEndpoint<Arc<Message<D>>>) {
        self.pusher
            .as_mut()
            .expect("Attempted to add endpoint to WriteStream, however no pusher exists")
            .add_endpoint(endpoint);
    }

    /// Closes the stream for future messages.
    fn close_stream(&mut self) {
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Closing write stream {} (ID: {})",
            self.get_name(),
            self.get_id()
        );
        self.stream_closed = true;
        self.pusher = None;
    }

    /// Updates the last watermark received on the stream.
    ///
    /// # Arguments
    /// * `msg` - The message to be sent on the stream.
    fn update_watermark(&mut self, msg: &Message<D>) -> Result<(), WriteStreamError> {
        match msg {
            Message::TimestampedData(td) => {
                if td.timestamp < self.low_watermark {
                    return Err(WriteStreamError::TimestampError);
                }
            }
            Message::Watermark(msg_watermark) => {
                if msg_watermark < &self.low_watermark {
                    return Err(WriteStreamError::TimestampError);
                }
                slog::debug!(
                    crate::TERMINAL_LOGGER,
                    "Updating watermark on WriteStream {} (ID: {}) from {:?} to {:?}",
                    self.get_name(),
                    self.get_id(),
                    self.low_watermark,
                    msg_watermark
                );
                self.low_watermark = msg_watermark.clone();
            }
        }
        Ok(())
    }
}

impl<D: Data> Default for WriteStream<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: Data> fmt::Debug for WriteStream<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WriteStream {{ id: {}, low_watermark: {:?} }}",
            self.id, self.low_watermark
        )
    }
}

impl<'a, D: Data + Deserialize<'a>> WriteStreamT<D> for WriteStream<D> {
    fn send(&mut self, msg: Message<D>) -> Result<(), WriteStreamError> {
        let send_time = Instant::now();

        // Check if the stream was closed before, and return an error.
        if self.stream_closed {
            slog::warn!(
                crate::TERMINAL_LOGGER,
                "Trying to send messages on a closed WriteStream {} (ID: {})",
                self.get_name(),
                self.get_id(),
            );
            return Err(WriteStreamError::Closed);
        }

        // Close the stream later if the message being sent represents the top watermark.
        let mut close_stream: bool = false;
        if msg.is_top_watermark() {
            slog::debug!(
                crate::TERMINAL_LOGGER,
                "Sending top watermark on the stream {} (ID: {}).",
                self.get_name(),
                self.get_id()
            );
            close_stream = true;
        }

        // Send notification.
        match &msg {
            Message::TimestampedData(_) => self
                .notification_tx
                .send(Notification::new(
                    send_time,
                    NotificationType::SentData(self.id, msg.timestamp().clone()),
                ))
                .unwrap_or(0),
            Message::Watermark(_) => self
                .notification_tx
                .send(Notification::new(
                    send_time,
                    NotificationType::SentWatermark(self.id, msg.timestamp().clone()),
                ))
                .unwrap_or(0),
        };

        // Update the watermark and send the message forward.
        self.update_watermark(&msg)?;
        let msg_arc = Arc::new(msg);

        match self.pusher.as_mut() {
            Some(pusher) => pusher.send(msg_arc).map_err(WriteStreamError::from)?,
            None => {
                slog::debug!(
                    crate::TERMINAL_LOGGER,
                    "No Pusher was found for the WriteStream {} (ID: {}). \
                             Skipping message sending.",
                    self.get_name(),
                    self.get_id()
                );
                ()
            }
        };

        // If we received a top watermark, close the stream.
        if close_stream {
            self.close_stream();
        }
        Ok(())
    }
}

impl<D: Data> Notifier for WriteStream<D> {
    fn subscribe(&self) -> broadcast::Receiver<Notification> {
        self.notification_tx.subscribe()
    }
}
