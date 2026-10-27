use std::hash::Hasher;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::task::Poll;

use crate::wire::{self};
use crate::zone::{ordering_from_u8, Catalog, ZoneConfig};

mod client;

pub use client::{
    ClientConfig, ClientConfigError, ConnectionStats, NetPeer, NetTransport, CONNECTS,
    DEFAULT_CLIENT_MAX_BYTES, DEFAULT_CLIENT_MAX_OPS, DEFAULT_CLIENT_NODE_MAX_BYTES,
    DEFAULT_CLIENT_NODE_MAX_OPS, DEFAULT_CONNECT_DEADLINE, DEFAULT_RPC_DEADLINE,
};

#[doc(hidden)]
pub use client::{
    connect_nonblocking, flush_dirty, AccountedBytes, ConnState, MuxConn, MuxRpc, Waiter,
};

#[doc(hidden)]
#[derive(Default)]
pub struct FxHasher {
    hash: u64,
}

const FX_SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl FxHasher {
    #[inline]
    fn add(&mut self, i: u64) {
        self.hash = (self.hash.rotate_left(5) ^ i).wrapping_mul(FX_SEED);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, mut bytes: &[u8]) {
        while bytes.len() >= 8 {
            self.add(u64::from_le_bytes(bytes[..8].try_into().unwrap()));
            bytes = &bytes[8..];
        }
        if !bytes.is_empty() {
            let mut buf = [0u8; 8];
            buf[..bytes.len()].copy_from_slice(bytes);
            self.add(u64::from_le_bytes(buf));
        }
    }
    #[inline]
    fn finish(&self) -> u64 {
        let mut z = self.hash;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

#[doc(hidden)]
pub enum Sock {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl Sock {
    pub fn as_raw_fd(&self) -> RawFd {
        match self {
            Sock::Tcp(s) => s.as_raw_fd(),
            Sock::Unix(s) => s.as_raw_fd(),
        }
    }
    pub fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Sock::Tcp(s) => (&*s).read(buf),
            Sock::Unix(s) => (&*s).read(buf),
        }
    }
    pub fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Sock::Tcp(s) => (&*s).write(buf),
            Sock::Unix(s) => (&*s).write(buf),
        }
    }
    pub fn peer_addr(&self) -> Option<std::net::SocketAddr> {
        match self {
            Sock::Tcp(s) => s.peer_addr().ok(),
            Sock::Unix(_) => None,
        }
    }
}

#[doc(hidden)]
pub enum Listener {
    Tcp(TcpListener),
    Unix(UnixListener),
}

impl Listener {
    pub fn as_raw_fd(&self) -> RawFd {
        match self {
            Listener::Tcp(l) => l.as_raw_fd(),
            Listener::Unix(l) => l.as_raw_fd(),
        }
    }
    pub fn accept_sock(&self) -> std::io::Result<Sock> {
        match self {
            Listener::Tcp(l) => {
                let (s, _) = l.accept()?;
                s.set_nonblocking(true)?;
                let _ = s.set_nodelay(true);
                Ok(Sock::Tcp(s))
            }
            Listener::Unix(l) => {
                let (s, _) = l.accept()?;
                s.set_nonblocking(true)?;
                Ok(Sock::Unix(s))
            }
        }
    }
}

#[doc(hidden)]
pub fn uds_path_for_port(port: u16) -> Option<PathBuf> {
    let dir = std::env::var_os("VALS_UDS_DIR")?;
    if dir.is_empty() {
        return None;
    }
    Some(std::path::Path::new(&dir).join(format!("vals-{port}.sock")))
}

#[allow(clippy::result_unit_err)]
#[doc(hidden)]
pub fn write_framed(out: &mut Vec<u8>, reqid: u64, payload: &[u8]) -> Result<(), ()> {
    let len = 8 + payload.len();
    if len > MAX_FRAME {
        return Err(());
    }
    out.extend_from_slice(&(len as u32).to_le_bytes());
    out.extend_from_slice(&reqid.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

#[allow(clippy::result_unit_err)]
#[doc(hidden)]
pub fn frame_msg(reqid: u64, payload: &[u8]) -> Result<Vec<u8>, ()> {
    if 8 + payload.len() > MAX_FRAME {
        return Err(());
    }
    let mut out = Vec::with_capacity(12 + payload.len());
    write_framed(&mut out, reqid, payload)?;
    Ok(out)
}

#[doc(hidden)]
pub fn catalog_from_wire(zones: Vec<wire::OwnedZone>) -> Catalog {
    let mut cat = Catalog::new();
    for z in zones {
        let Some(ordering) = ordering_from_u8(z.ordering) else {
            continue;
        };
        cat.insert(
            z.id,
            ZoneConfig {
                name: z.name,
                rf: z.rf as usize,
                ordering,
                scope: z.scope.map(|s| s.into_iter().collect()),
            },
        );
    }
    cat
}

#[doc(hidden)]
pub struct FrameRead {
    buf: Vec<u8>,
    start: usize,
    scratch: Box<[u8]>,
}

const READ_CHUNK: usize = 32 * 1024;

#[doc(hidden)]
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

#[doc(hidden)]
pub const MAX_CONN_SEND_BUF: usize = 256 * 1024 * 1024;

#[doc(hidden)]
pub const SCAN_REPLY_BUDGET: usize = 8 * 1024 * 1024;

#[doc(hidden)]
pub const MAX_CONN_INFLIGHT_READS: usize = 1024;

impl Default for FrameRead {
    fn default() -> Self {
        FrameRead {
            buf: Vec::new(),
            start: 0,
            scratch: vec![0u8; READ_CHUNK].into_boxed_slice(),
        }
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameErr {
    Closed,
    Overloaded,
    Oversized(usize),
}

impl FrameRead {
    pub fn reset(&mut self) {
        self.buf.clear();
        self.start = 0;
    }

    pub fn read_block(&mut self, stream: &Sock) -> Poll<Result<(usize, usize), FrameErr>> {
        self.read_block_accounted(stream, |_| true, |_| {})
    }

    #[doc(hidden)]
    pub fn read_block_accounted(
        &mut self,
        stream: &Sock,
        mut reserve: impl FnMut(usize) -> bool,
        mut release: impl FnMut(usize),
    ) -> Poll<Result<(usize, usize), FrameErr>> {
        loop {
            let avail = self.buf.len() - self.start;
            if avail >= 4 {
                let len =
                    u32::from_le_bytes(self.buf[self.start..self.start + 4].try_into().unwrap())
                        as usize;
                if len > MAX_FRAME {
                    return Poll::Ready(Err(FrameErr::Oversized(len)));
                }
                if avail >= 4 + len {
                    let off = self.start + 4;
                    self.start += 4 + len;
                    return Poll::Ready(Ok((off, len)));
                }
            }
            if self.start > 0 {
                self.buf.drain(..self.start);
                self.start = 0;
            }
            match stream.read(&mut self.scratch) {
                Ok(0) => return Poll::Ready(Err(FrameErr::Closed)),
                Ok(n) => {
                    if !reserve(n) {
                        return Poll::Ready(Err(FrameErr::Overloaded));
                    }
                    let old_capacity = self.buf.capacity();
                    if self.buf.try_reserve_exact(n).is_err() {
                        release(n);
                        return Poll::Ready(Err(FrameErr::Overloaded));
                    }
                    let actual = self.buf.capacity() - old_capacity;
                    if actual > n && !reserve(actual - n) {
                        self.buf = Vec::new();
                        release(old_capacity + n);
                        return Poll::Ready(Err(FrameErr::Overloaded));
                    }
                    if actual < n {
                        release(n - actual);
                    }
                    self.buf.extend_from_slice(&self.scratch[..n]);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Poll::Pending,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => return Poll::Ready(Err(FrameErr::Closed)),
            }
        }
    }

    pub fn frame_at(&self, off: usize, len: usize) -> &[u8] {
        &self.buf[off..off + len]
    }

    pub fn buffered_len(&self) -> usize {
        self.buf.len().saturating_sub(self.start)
    }
}

#[cfg(test)]
mod frame_read_tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn accounted_reader_keeps_retained_high_water_charged_and_reserves_before_append() {
        let (reader, mut writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let reader = Sock::Unix(reader);
        let payload = vec![7u8; 4096];
        let mut input = (payload.len() as u32).to_le_bytes().to_vec();
        input.extend_from_slice(&payload);
        input.extend_from_slice(&100u32.to_le_bytes());
        input.extend_from_slice(&[1, 2, 3, 4, 5]);
        writer.write_all(&input).unwrap();

        let used = Cell::new(0usize);
        let mut frames = FrameRead::default();
        let first = frames.read_block_accounted(
            &reader,
            |bytes| {
                used.set(used.get() + bytes);
                true
            },
            |bytes| used.set(used.get() - bytes),
        );
        assert!(matches!(first, Poll::Ready(Ok((_, 4096)))));
        assert!(used.get() >= 4 + 4096 + 4 + 5);
        let high_water = used.get();
        let second = frames.read_block_accounted(
            &reader,
            |bytes| {
                used.set(used.get() + bytes);
                true
            },
            |bytes| used.set(used.get() - bytes),
        );
        assert_eq!(second, Poll::Pending);
        assert_eq!(used.get(), high_water);

        let (reader, mut writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let reader = Sock::Unix(reader);
        writer.write_all(&100u32.to_le_bytes()).unwrap();
        writer.write_all(&[0u8; 100]).unwrap();
        let mut rejected = FrameRead::default();
        let result = rejected.read_block_accounted(&reader, |bytes| bytes <= 32, |_| {});
        assert_eq!(result, Poll::Ready(Err(FrameErr::Overloaded)));
        assert_eq!(rejected.buf.capacity(), 0);
    }
}
