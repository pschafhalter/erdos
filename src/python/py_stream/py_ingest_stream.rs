use pyo3::{exceptions, prelude::*};

use crate::{
    dataflow::{
        stream::{IngestStream, ReadStream, WriteStreamT},
        Message,
    },
    node::NodeId,
    python::PyMessage,
};

use super::PyReadStream;

#[pyclass]
pub struct PyIngestStream {
    ingest_stream: IngestStream<Vec<u8>>,
}

#[pymethods]
impl PyIngestStream {
    #[new]
    fn new(obj: &PyRawObject, node_id: NodeId) {
        obj.init(Self {
            ingest_stream: IngestStream::new(node_id),
        });
    }

    fn is_closed(&self) -> bool {
        self.ingest_stream.is_closed()
    }

    fn send(&mut self, msg: &PyMessage) -> PyResult<()> {
        let erdos_msg = Message::from(msg);
        let msg_size: usize = erdos_msg.data().map(|d| d.len()).unwrap_or(0);
        slog::info!(
            crate::get_terminal_logger(),
            "WriteStream {}: sending message with data size {}, is_watermark={}",
            self.ingest_stream.get_id(),
            msg_size,
            msg.is_watermark()
        );

        self.ingest_stream.send(erdos_msg).map_err(|e| {
            exceptions::Exception::py_err(format!(
                "Error sending message on ingest stream {}: {:?}",
                self.ingest_stream.get_id(),
                e
            ))
        })
    }

    fn to_py_read_stream(&self) -> PyReadStream {
        PyReadStream::from(ReadStream::from(&self.ingest_stream))
    }
}
