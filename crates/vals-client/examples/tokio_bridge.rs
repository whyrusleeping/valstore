//! A Tokio integration pattern, not a throughput API: one dedicated OS thread
//! owns the !Send coordinator/transport and serializes all operations. Tokio
//! tasks share only a Send handle, a bounded mpsc inbox and oneshot replies.
//!
//! ```text
//! cargo run --locked -p vals-client --example tokio_bridge
//! cargo run --locked -p vals-client --example tokio_bridge -- --nodes 1@127.0.0.1:7001,2@127.0.0.1:7002,3@127.0.0.1:7003
//! ```
//!
//! Only the default zone is used. Startup discovers membership and the catalog
//! from seeds, then derives R = RF / 2 + 1. Puts/deletes request device-durable
//! majority acknowledgements; finite transport connect/RPC deadlines stay on.
//! Reads select the greatest LWW Version (HLC, then writer id), not the last
//! request by wall-clock completion. An acknowledged write may lose to a
//! greater Version, and a failed/timed-out write may still have taken effect.
//! This is not a linearizable read-modify-write interface. The coordinator's
//! automatic stale-ring refresh/retry stays enabled, but R is a startup
//! snapshot; reconnect the bridge if the default zone's RF changes.
//!
//! At most QUEUE_CAPACITY queued operations plus one running operation are
//! accepted. There is no byte cap: payloads and tasks waiting for admission
//! still consume caller memory. Bound those separately in a real application.
//! Cancelling a caller can skip queued work, but never retracts a started write.
//! Shutdown closes admission, drains accepted non-cancelled work, and joins via
//! spawn_blocking even if handle clones remain alive. Dropping the owner also
//! requests shutdown, but only explicit shutdown observes termination/errors.
//!
//! The `vals_client` crate documentation states the threading, retry,
//! cancellation and consistency rules this pattern is built around.

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread;

use tokio::sync::{mpsc, oneshot};
use vals_client::{
    reactor, Coordinator, NetTransport, NodeId, Ring, Stored, Version,
    WriteConcern, WriteOptions, DEFAULT_ZONE,
};

const DEFAULT_NODES: &str = "1@127.0.0.1:7001,2@127.0.0.1:7002,3@127.0.0.1:7003";
const QUEUE_CAPACITY: usize = 32;
const USAGE: &str = "usage: tokio_bridge [--nodes ID@HOST:PORT,...]";
type Result<T> = std::result::Result<T, String>;
type Nodes = Vec<(NodeId, Vec<SocketAddr>)>;

enum Operation {
    Put(Vec<u8>, Vec<u8>),
    Get(Vec<u8>),
    Delete(Vec<u8>),
}

#[derive(Debug)]
enum Reply {
    Version(Version),
    Value(Option<Stored>),
}

struct Request {
    operation: Operation,
    reply: oneshot::Sender<Result<Reply>>,
}

/// Clone this into Tokio tasks; no vals client state crosses the thread boundary.
#[derive(Clone)]
struct Client {
    requests: mpsc::Sender<Request>,
    accepting: Arc<Mutex<bool>>,
}

impl Client {
    /// Admission commits at permit.send under the gate shared with shutdown.
    async fn enqueue(&self, operation: Operation) -> Result<oneshot::Receiver<Result<Reply>>> {
        if !*self.accepting.lock().unwrap() {
            return Err("bridge admission is closed".into());
        }
        let permit = self.requests.reserve().await
            .map_err(|_| "bridge request channel closed (worker stopped)".to_string())?;
        let accepting = self.accepting.lock().unwrap();
        if !*accepting {
            return Err("bridge admission is closed".into());
        }
        let (reply, receive) = oneshot::channel();
        permit.send(Request { operation, reply });
        Ok(receive)
    }

    async fn call(&self, operation: Operation) -> Result<Reply> {
        self.enqueue(operation).await?.await
            .map_err(|_| "worker stopped before replying; write outcome may be unknown".to_string())?
    }

    async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<Version> {
        match self.call(Operation::Put(key, value)).await? {
            Reply::Version(version) => Ok(version),
            _ => Err("worker returned an unexpected put reply".into()),
        }
    }

    async fn get(&self, key: Vec<u8>) -> Result<Option<Stored>> {
        match self.call(Operation::Get(key)).await? {
            Reply::Value(value) => Ok(value),
            _ => Err("worker returned an unexpected get reply".into()),
        }
    }

    async fn delete(&self, key: Vec<u8>) -> Result<Version> {
        match self.call(Operation::Delete(key)).await? {
            Reply::Version(version) => Ok(version),
            _ => Err("worker returned an unexpected delete reply".into()),
        }
    }
}

/// A single lifecycle owner, separate from the cloneable operation handle.
struct Bridge {
    client: Client,
    stop: Option<oneshot::Sender<()>>,
    worker: Option<thread::JoinHandle<Result<()>>>,
}

impl Bridge {
    async fn connect(nodes: String) -> Result<Self> {
        Self::start(QUEUE_CAPACITY, move || initialize(&nodes)).await
    }

    /// The initializer and handler run only on the worker; H need not be Send.
    async fn start<I, H>(capacity: usize, initialize: I) -> Result<Self>
    where
        I: FnOnce() -> Result<H> + Send + 'static,
        H: FnMut(Operation) -> Result<Reply> + 'static,
    {
        if capacity == 0 {
            return Err("queue capacity must be positive".into());
        }
        let (requests, mut receive) = mpsc::channel::<Request>(capacity);
        let (stop, mut stopping) = oneshot::channel();
        let (ready, started) = oneshot::channel();
        let worker = thread::Builder::new().name("vals-client-bridge".into()).spawn(move || {
            let mut handle = match initialize() {
                Ok(handle) => handle,
                Err(error) => {
                    let _ = ready.send(Err(error));
                    return Ok(());
                }
            };
            let inbox_runtime = tokio::runtime::Builder::new_current_thread().build()
                .map_err(|e| format!("worker inbox runtime: {e}"))?;
            if ready.send(Ok(())).is_err() {
                return Ok(());
            }
            inbox_runtime.block_on(async {
                let mut draining = false;
                loop {
                    let request = tokio::select! {
                        biased;
                        _ = &mut stopping, if !draining => {
                            receive.close();
                            draining = true;
                            continue;
                        }
                        request = receive.recv() => request,
                    };
                    let Some(request) = request else { break };
                    if !request.reply.is_closed() {
                        let result = handle(request.operation);
                        let _ = request.reply.send(result);
                    }
                }
            });
            Ok(())
        }).map_err(|e| format!("spawning worker: {e}"))?;
        let bridge = Self {
            client: Client { requests, accepting: Arc::new(Mutex::new(true)) },
            stop: Some(stop),
            worker: Some(worker),
        };
        match started.await {
            Ok(Ok(())) => Ok(bridge),
            Ok(Err(error)) => {
                bridge.shutdown().await?;
                Err(format!("initializing client: {error}"))
            }
            Err(_) => {
                bridge.shutdown().await?;
                Err("worker exited without reporting startup".into())
            }
        }
    }

    fn client(&self) -> Client {
        self.client.clone()
    }

    fn close(&mut self) {
        *self.client.accepting.lock().unwrap() = false;
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }

    /// Once polled, closing is irrevocable; cancelling the join does not stop it.
    async fn shutdown(mut self) -> Result<()> {
        self.close();
        let worker = self.worker.take().unwrap();
        tokio::task::spawn_blocking(move || worker.join())
            .await.map_err(|e| format!("joining worker task: {e}"))?
            .map_err(|panic| {
                let message = panic.downcast_ref::<String>().map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("non-string panic payload");
                format!("worker panicked: {message}")
            })?
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.close();
    }
}

fn parse_nodes(list: &str) -> Result<Nodes> {
    let mut nodes: Nodes = Vec::new();
    for item in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (id, addr) = item.split_once('@')
            .ok_or_else(|| format!("bad node `{item}`: expected ID@HOST:PORT"))?;
        let id = id.trim().parse::<NodeId>()
            .map_err(|_| format!("bad node id in `{item}`"))?;
        let addr = addr.trim().to_socket_addrs()
            .map_err(|e| format!("bad address in `{item}`: {e}"))?
            .next().ok_or_else(|| format!("address in `{item}` did not resolve"))?;
        match nodes.iter_mut().find(|(node, _)| *node == id) {
            Some((_, addrs)) => addrs.push(addr),
            None => nodes.push((id, vec![addr])),
        }
    }
    if nodes.is_empty() {
        return Err("--nodes lists no nodes".into());
    }
    Ok(nodes)
}

/// DNS, transport construction and every vals future stay on the OS thread.
/// Tokio only parks the idle inbox; vals-rt drives network I/O for each RPC.
fn initialize(nodes: &str) -> Result<impl FnMut(Operation) -> Result<Reply>> {
    let nodes = parse_nodes(nodes)?;
    let transport = NetTransport::connect_sharded(nodes.clone());
    let coord = Coordinator::new(100, Ring::new(3), 2, transport);
    let majority = vals_rt::block_on(discover(&coord, &nodes), reactor::poll_io)?;
    Ok(move |operation| {
        vals_rt::block_on(async {
            let durable = WriteOptions::device_durable(WriteConcern::Majority);
            match operation {
                Operation::Put(key, value) => coord.write_in(DEFAULT_ZONE, &key, &value, durable)
                    .await.map(Reply::Version).map_err(|e| format!("put: {e}")),
                Operation::Get(key) => coord.read_quorum_in(DEFAULT_ZONE, &key, majority)
                    .await.map(Reply::Value).map_err(|e| format!("get: {e}")),
                Operation::Delete(key) => coord.delete_in(DEFAULT_ZONE, &key, durable)
                    .await.map(Reply::Version).map_err(|e| format!("delete: {e}")),
            }
        }, reactor::poll_io)
    })
}

async fn discover(coord: &Coordinator<NetTransport>, nodes: &Nodes) -> Result<usize> {
    let mut errors = Vec::new();
    for (node, _) in nodes {
        let result: Result<usize> = async {
            let members = coord.refresh_ring(*node).await.map_err(|e| e.to_string())?;
            if members.is_empty() || coord.ring_epoch() == 0 {
                return Err("no authoritative membership yet (is the PD running?)".into());
            }
            coord.refresh_catalog(*node).await.map_err(|e| e.to_string())?;
            let rf = coord.catalog().get(DEFAULT_ZONE)
                .ok_or("default zone is missing")?.rf;
            if rf == 0 {
                return Err("default zone RF must be positive".into());
            }
            Ok(rf / 2 + 1)
        }.await;
        match result {
            Ok(majority) => return Ok(majority),
            Err(error) => errors.push(format!("seed {node}: {error}")),
        }
    }
    Err(format!("membership/catalog discovery failed: {}", errors.join("; ")))
}

async fn round_trip(client: Client, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
    let written = client.put(key.clone(), value.clone()).await?;
    let stored = client.get(key.clone()).await?.ok_or("put was not found")?;
    if stored.value.as_deref() != Some(value.as_slice()) || stored.version != written {
        return Err(format!("round-trip mismatch for {}: {stored:?}", String::from_utf8_lossy(&key)));
    }
    let deleted = client.delete(key.clone()).await?;
    if deleted <= written || client.get(key.clone()).await?.is_some() {
        return Err(format!("delete check failed for {}", String::from_utf8_lossy(&key)));
    }
    println!("{}: round-trip and delete OK ({written:?} -> {deleted:?})", String::from_utf8_lossy(&key));
    Ok(())
}

async fn demo(bridge: &Bridge) -> Result<()> {
    let prefix = format!("tokio-bridge/{}/{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?.as_nanos());
    let mut tasks = Vec::new();
    for index in 0..4 {
        let client = bridge.client();
        let key = format!("{prefix}/{index}").into_bytes();
        tasks.push(tokio::spawn(async move {
            round_trip(client, key, format!("hello from task {index}").into_bytes()).await
        }));
    }
    let mut failures = Vec::new();
    for task in tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(error),
            Err(error) => failures.push(format!("demo task: {error}")),
        }
    }
    if failures.is_empty() { Ok(()) } else { Err(failures.join("; ")) }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let mut nodes = DEFAULT_NODES.to_string();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}\ndefault seeds: {DEFAULT_NODES}");
                return std::process::ExitCode::SUCCESS;
            }
            "--nodes" => match args.next() {
                Some(value) => nodes = value,
                None => {
                    eprintln!("--nodes needs a value\n{USAGE}");
                    return std::process::ExitCode::from(2);
                }
            },
            _ => {
                eprintln!("unknown argument: {arg}\n{USAGE}");
                return std::process::ExitCode::from(2);
            }
        }
    }
    let result = async {
        let bridge = Bridge::connect(nodes).await?;
        let demo_result = demo(&bridge).await;
        let shutdown_result = bridge.shutdown().await;
        match (demo_result, shutdown_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(demo), Err(shutdown)) => Err(format!("{demo}; shutdown: {shutdown}")),
        }
    }.await;
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("tokio_bridge: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::future::Future;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn replies_cross_threads_with_non_send_worker_state() {
        let caller_thread = thread::current().id();
        let bridge = Bridge::start(2, move || {
            assert_ne!(thread::current().id(), caller_thread);
            let values = Rc::new(RefCell::new(HashMap::new()));
            let mut hlc = 0;
            Ok(move |operation| {
                hlc += 1;
                let version = Version { hlc, node: 100 };
                match operation {
                    Operation::Put(key, value) => {
                        values.borrow_mut().insert(key, Stored { version, value: Some(value) });
                        Ok(Reply::Version(version))
                    }
                    Operation::Get(key) => Ok(Reply::Value(values.borrow().get(&key).cloned())),
                    Operation::Delete(key) => {
                        values.borrow_mut().remove(&key);
                        Ok(Reply::Version(version))
                    }
                }
            })
        }).await.unwrap();
        let client = bridge.client();
        tokio::spawn(round_trip(client, b"key".to_vec(), b"value".to_vec()))
            .await.unwrap().unwrap();
        bridge.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_queued_work_is_skipped_but_started_write_finishes() {
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let handled = Arc::new(AtomicUsize::new(0));
        let count = handled.clone();
        let bridge = Bridge::start(2, move || {
            let mut entered = Some(entered);
            Ok(move |_| {
                if let Some(entered) = entered.take() {
                    entered.send(()).unwrap();
                    wait.recv().unwrap();
                }
                count.fetch_add(1, Ordering::SeqCst);
                Ok(Reply::Version(Version { hlc: 1, node: 100 }))
            })
        }).await.unwrap();
        let client = bridge.client();
        let running = client.enqueue(Operation::Put(vec![1], vec![1])).await.unwrap();
        started.await.unwrap();
        let queued = client.enqueue(Operation::Put(vec![2], vec![2])).await.unwrap();
        drop(running);
        drop(queued);
        release.send(()).unwrap();
        bridge.shutdown().await.unwrap();
        assert_eq!(handled.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn bounded_admission_and_shutdown_drain_with_live_clones() {
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let handled = Arc::new(AtomicUsize::new(0));
        let count = handled.clone();
        let bridge = Bridge::start(1, move || {
            let mut entered = Some(entered);
            Ok(move |_| {
                if let Some(entered) = entered.take() {
                    entered.send(()).unwrap();
                    wait.recv().unwrap();
                }
                count.fetch_add(1, Ordering::SeqCst);
                Ok(Reply::Value(None))
            })
        }).await.unwrap();
        let client = bridge.client();
        let clone = client.clone();
        let first = client.enqueue(Operation::Get(vec![1])).await.unwrap();
        started.await.unwrap();
        let second = client.enqueue(Operation::Get(vec![2])).await.unwrap();
        assert_eq!(client.requests.capacity(), 0);
        let mut blocked = Box::pin(clone.enqueue(Operation::Get(vec![3])));
        assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(blocked.as_mut().poll(cx).is_pending())).await);
        let mut shutdown = Box::pin(bridge.shutdown());
        assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(shutdown.as_mut().poll(cx).is_pending())).await);
        assert!(client.get(vec![4]).await.unwrap_err().contains("admission is closed"));
        release.send(()).unwrap();
        assert!(blocked.await.is_err());
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        shutdown.await.unwrap();
        assert_eq!(handled.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn initialization_operation_and_worker_failures_reach_callers() {
        let failed = Bridge::start(1, || {
            Err::<fn(Operation) -> Result<Reply>, _>("injected init error".into())
        }).await;
        assert!(failed.err().unwrap().contains("injected init error"));
        let panicked = Bridge::start(1, || -> Result<fn(Operation) -> Result<Reply>> {
            panic!("injected startup panic")
        }).await;
        assert!(panicked.err().unwrap().contains("injected startup panic"));
        let bridge = Bridge::start(1, || Ok(|operation| match operation {
            Operation::Get(_) => Err("injected operation error".into()),
            _ => panic!("injected worker panic"),
        })).await.unwrap();
        let client = bridge.client();
        assert_eq!(client.get(vec![]).await.unwrap_err(), "injected operation error");
        assert!(client.put(vec![], vec![]).await.unwrap_err().contains("worker stopped"));
        client.requests.closed().await;
        assert!(client.get(vec![]).await.unwrap_err().contains("channel closed"));
        assert!(bridge.shutdown().await.unwrap_err().contains("injected worker panic"));
    }

    #[tokio::test]
    async fn dropping_owner_closes_admission_despite_live_clone() {
        let (exited, exit) = oneshot::channel();
        struct Exit(Option<oneshot::Sender<()>>);
        impl Drop for Exit {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let bridge = Bridge::start(1, move || {
            let exit = Exit(Some(exited));
            Ok(move |_| {
                let _ = &exit;
                Ok(Reply::Value(None))
            })
        }).await.unwrap();
        let client = bridge.client();
        drop(bridge);
        assert!(client.get(vec![]).await.unwrap_err().contains("admission is closed"));
        exit.await.unwrap();
    }

    #[tokio::test]
    async fn refused_seed_reports_real_discovery_failure() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let error = Bridge::connect(format!("1@{addr}")).await.err().unwrap();
        assert!(error.contains("discovery failed"), "{error}");
    }

    #[test]
    fn seed_addresses_group_shards_and_reject_bad_input() {
        let nodes = parse_nodes("1@127.0.0.1:7001,1@127.0.0.1:7002,2@127.0.0.1:7003").unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].1.len(), 2);
        assert!(parse_nodes("").is_err());
        assert!(parse_nodes("bad").is_err());
        assert!(parse_nodes("x@127.0.0.1:7001").is_err());
    }
}
