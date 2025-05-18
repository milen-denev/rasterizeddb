use io_uring::{IoUring, opcode, types};
use std::{
    fs::File,
    io,
    os::fd::AsRawFd,
    sync::Arc,
    thread,
};

use crossbeam_channel::{unbounded, Receiver, Sender};
use futures::channel::oneshot;

/// A single read request sent to the background io_uring thread.
struct ReadRequest {
    offset: u64,
    buffer: Vec<u8>,
    responder: oneshot::Sender<io::Result<Vec<u8>>>,
}

/// An async, thread-safe io_uring reader.
/// Cloneable, Send + Sync.
#[derive(Clone)]
pub struct AsyncUringReader {
    tx: Arc<Sender<ReadRequest>>,
}

impl AsyncUringReader {
    /// Create a new reader on `file_path`, with a queue depth of `queue_depth`.
    /// Spawns a background thread that drives the io_uring.
    pub fn new(file_path: &str, queue_depth: u32) -> io::Result<Self> {
        // 1) Initialize the ring
        let ring = IoUring::new(queue_depth)?;
        // 2) Open the file (we'll move this into the thread so it isn't dropped)
        let file = File::open(file_path)?;
        // 3) Register the raw FD so io_uring can index it
        let raw_fd = file.as_raw_fd();
        ring.submitter().register_files(&[raw_fd])?;

        // Keep a types::Fd wrapper for submissions
        let fd_wrapper = types::Fd(raw_fd);

        // 4) Create channel for requests
        let (tx, rx): (Sender<ReadRequest>, Receiver<ReadRequest>) = unbounded();

        // 5) Move ring, file, and wrapper into the thread so File isn't closed
        thread::spawn(move || {
            process_loop(ring, fd_wrapper, rx)
        });

        Ok(AsyncUringReader { tx: Arc::new(tx) })
    }
    
    /// Asynchronously read `size` bytes at `offset`.
    /// Returns a Vec<u8> of exactly `size` length on success.
    pub async fn read_at(&self, offset: u64, size: usize) -> io::Result<Vec<u8>> {
        let buffer = vec![0u8; size];
        let (resp_tx, resp_rx) = oneshot::channel();

        let req = ReadRequest {
            offset,
            buffer,
            responder: resp_tx,
        };
        self.tx
            .send(req)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "uring thread stopped"))?;

        resp_rx
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "uring thread panicked"))?
    }
}

fn process_loop(
    mut ring: IoUring,
    fd: types::Fd,
    rx: Receiver<ReadRequest>,
) {
    // `file` stays alive here so its FD never closes
    for mut req in rx.iter() {
        let len = req.buffer.len().min(u32::MAX as usize) as u32;
        let sqe = opcode::Read::new(fd, req.buffer.as_mut_ptr(), len)
            .offset(req.offset)
            .build()
            .user_data(0xDEAD_BEEF);

        unsafe {
            if let Err(_) = ring.submission().push(&sqe) {
                let _ = req.responder.send(Err(io::Error::new(
                    io::ErrorKind::Other,
                    "SQE queue full",
                )));
                continue;
            }
        }

        if let Err(e) = ring.submit_and_wait(1) {
            let _ = req.responder.send(Err(e));
            continue;
        }

        if let Some(cqe) = ring.completion().next() {
            let result = cqe.result();
            if result < 0 {
                let err = io::Error::from_raw_os_error(-result);
                let _ = req.responder.send(Err(err));
            } else {
                let n = result as usize;
                req.buffer.truncate(n);
                let _ = req.responder.send(Ok(req.buffer));
            }
        } else {
            let _ = req.responder.send(Err(io::Error::new(
                io::ErrorKind::Other,
                "completion queue empty",
            )));
        }
    }
}