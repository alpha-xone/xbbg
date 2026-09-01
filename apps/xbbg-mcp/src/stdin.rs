//! Standard-input transport for the stdio server.
//!
//! `rmcp::transport::stdio()` wraps `tokio::io::stdin()`, whose adapter reports every zero-byte
//! read as end-of-input and turns read errors into a silent `None`; the service loop then exits
//! with status 0 and nothing on stderr. Issue #348 is exactly that shape: on one Windows host the
//! server answers `initialize` and vanishes as soon as stdin goes quiet, and the bare exit left
//! nothing to diagnose. This reader runs the blocking reads on its own thread, only reports EOF
//! when stdin is really closed, and says on stderr why it stopped.

use std::io::{self, Read};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc;

const CHUNK_BYTES: usize = 64 * 1024;
const CHANNEL_CHUNKS: usize = 16;
const ZERO_READ_BACKOFF_MAX: Duration = Duration::from_millis(100);

/// Start the reader thread and return the async side of the stream.
pub(crate) fn stdin() -> Stdin {
    let (tx, rx) = mpsc::channel(CHANNEL_CHUNKS);
    thread::Builder::new()
        .name("xbbg-mcp-stdin".into())
        .spawn(move || read_loop(tx))
        .expect("spawn stdin reader thread");
    Stdin {
        rx,
        pending: Vec::new(),
        offset: 0,
    }
}

fn read_loop(tx: mpsc::Sender<io::Result<Vec<u8>>>) {
    let mut stdin = io::stdin().lock();
    let mut backoff = Duration::from_millis(1);
    let mut zero_reads_ignored = 0u32;
    loop {
        let mut chunk = vec![0u8; CHUNK_BYTES];
        match stdin.read(&mut chunk) {
            Ok(0) => {
                if stdin_pipe_is_open() {
                    // A zero-byte read on a still-connected pipe is not end-of-input. Windows
                    // reports one after a zero-length write by the peer; `std` folds it into
                    // `Ok(0)` and every layer above then shuts down. Wait briefly and read again.
                    if zero_reads_ignored == 0 {
                        eprintln!("xbbg-mcp: ignoring zero-byte read on open stdin pipe");
                    }
                    zero_reads_ignored += 1;
                    thread::sleep(backoff);
                    backoff = (backoff * 2).min(ZERO_READ_BACKOFF_MAX);
                    continue;
                }
                eprintln!("xbbg-mcp: stdin closed; shutting down");
                return;
            }
            Ok(n) => {
                chunk.truncate(n);
                backoff = Duration::from_millis(1);
                if tx.blocking_send(Ok(chunk)).is_err() {
                    return; // service loop is gone
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => {
                eprintln!("xbbg-mcp: stdin read failed: {err}; shutting down");
                let _ = tx.blocking_send(Err(err));
                return;
            }
        }
    }
}

/// `true` when stdin is a pipe whose write end is still open.
///
/// `PeekNamedPipe` succeeds (with zero bytes available) while a writer holds the other end and
/// fails with `ERROR_BROKEN_PIPE` once every writer has gone, so it separates "nothing to read
/// right now" from end-of-input where `ReadFile` alone does not.
#[cfg(windows)]
fn stdin_pipe_is_open() -> bool {
    use std::os::windows::io::AsRawHandle;
    use std::ptr;

    type Handle = *mut core::ffi::c_void;
    const FILE_TYPE_PIPE: u32 = 0x0003;

    #[link(name = "kernel32")]
    extern "system" {
        fn GetFileType(handle: Handle) -> u32;
        fn PeekNamedPipe(
            handle: Handle,
            buffer: *mut core::ffi::c_void,
            buffer_size: u32,
            bytes_read: *mut u32,
            total_bytes_avail: *mut u32,
            bytes_left_this_message: *mut u32,
        ) -> i32;
    }

    let handle = io::stdin().as_raw_handle() as Handle;
    // SAFETY: both calls take a process-owned handle and, with a null buffer and zero size,
    // write nothing; every out-pointer is documented as optional.
    unsafe {
        GetFileType(handle) == FILE_TYPE_PIPE
            && PeekNamedPipe(
                handle,
                ptr::null_mut(),
                0,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
            ) != 0
    }
}

/// POSIX `read` returns 0 on a pipe only at end-of-input.
#[cfg(not(windows))]
fn stdin_pipe_is_open() -> bool {
    false
}

/// Async side of the stdin reader; feed it to `rmcp` in place of `tokio::io::Stdin`.
pub(crate) struct Stdin {
    rx: mpsc::Receiver<io::Result<Vec<u8>>>,
    pending: Vec<u8>,
    offset: usize,
}

impl AsyncRead for Stdin {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.offset >= self.pending.len() {
            match self.rx.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(())), // EOF: reader thread finished
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(Some(Ok(chunk))) => {
                    self.pending = chunk;
                    self.offset = 0;
                }
            }
        }
        let this = &mut *self;
        let available = &this.pending[this.offset..];
        let n = available.len().min(buf.remaining());
        buf.put_slice(&available[..n]);
        this.offset += n;
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn stream(chunks: Vec<io::Result<Vec<u8>>>) -> Stdin {
        let (tx, rx) = mpsc::channel(CHANNEL_CHUNKS);
        for chunk in chunks {
            tx.try_send(chunk).expect("channel has room");
        }
        Stdin {
            rx,
            pending: Vec::new(),
            offset: 0,
        }
    }

    #[tokio::test]
    async fn chunks_are_delivered_in_order_and_split_across_small_reads() {
        let mut stdin = stream(vec![Ok(b"hello ".to_vec()), Ok(b"world".to_vec())]);
        let mut small = [0u8; 4];
        let mut out = Vec::new();
        loop {
            let n = stdin.read(&mut small).await.unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&small[..n]);
        }
        assert_eq!(out, b"hello world");
    }

    #[tokio::test]
    async fn read_error_surfaces_before_eof() {
        let mut stdin = stream(vec![
            Ok(b"x".to_vec()),
            Err(io::Error::other("stdin handle invalidated")),
        ]);
        let mut buf = [0u8; 8];
        assert_eq!(stdin.read(&mut buf).await.unwrap(), 1);
        let err = stdin.read(&mut buf).await.unwrap_err();
        assert_eq!(err.to_string(), "stdin handle invalidated");
        assert_eq!(stdin.read(&mut buf).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn closed_channel_is_eof() {
        let mut stdin = stream(vec![]);
        let mut buf = [0u8; 8];
        assert_eq!(stdin.read(&mut buf).await.unwrap(), 0);
    }
}
