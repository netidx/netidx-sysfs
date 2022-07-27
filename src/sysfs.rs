use anyhow::{bail, Result};
use futures::{
    channel::{
        mpsc::{self, UnboundedSender},
        oneshot,
    },
    prelude::*,
    select_biased,
};
use fxhash::FxHashMap;
use log::error;
use netidx::{
    pool::{Pool, Pooled},
    publisher::Value,
};
use std::{
    collections::{BTreeMap, HashMap},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{sync::broadcast, task::JoinHandle, time};
use tokio_uring::{
    self,
    buf::{IoBuf, IoBufMut},
    fs::File,
};

struct Buf(Pooled<Vec<u8>>);

impl Deref for Buf {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl DerefMut for Buf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.0
    }
}

unsafe impl IoBuf for Buf {
    fn stable_ptr(&self) -> *const u8 {
        self.0.stable_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.0.bytes_init()
    }

    fn bytes_total(&self) -> usize {
        self.0.bytes_total()
    }
}

unsafe impl IoBufMut for Buf {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.0.stable_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        self.0.set_init(pos)
    }
}

lazy_static! {
    static ref BUF: Pool<Vec<u8>> = Pool::new(100_000, 4096);
}

async fn read_file(file: &Path) -> Result<Buf> {
    const C: usize = 512;
    let fd = File::open(file).await?;
    let mut pos = 0;
    let mut buf = Buf(BUF.take());
    loop {
        {
            let len = buf.len();
            if len - pos < C {
                buf.resize(len + C, 0);
            }
        }
        let (read, res) = fd.read_at(buf.slice(pos..), pos as u64).await;
        let read = read?;
        if read == 0 {
            return Ok(res.into_inner());
        } else {
            buf = res.into_inner();
            pos += read;
        }
    }
}

pub(crate) async fn poll_file(
    file: PathBuf,
    id: Fid,
    updates: UnboundedSender<(Fid, Value)>,
    first: oneshot::Sender<Value>,
    mut clock: broadcast::Receiver<()>,
) -> Result<()> {
    let mut first = Some(first);
    let mut send = |v: Value| -> Result<()> {
        match first.take() {
            None => Ok(updates.unbounded_send((id, v))?),
            Some(first) => {
                let _ = first.send(v);
                Ok(())
            }
        }
    };
    loop {
        let data = read_file(&file).await?;
        let data = String::from_utf8_lossy(&**data);
        match data.parse::<i64>() {
            Ok(i) => send(Value::from(i))?,
            Err(_) => match data.parse::<f64>() {
                Ok(f) => send(Value::from(f))?,
                Err(_) => send(Value::from(data.into_owned()))?,
            },
        }
        if let Err(_) = clock.recv().await {
            break Ok(());
        }
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) struct Fid(u64);

impl Fid {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Fid(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

enum FileReq {
    StartPolling(Fid, PathBuf, oneshot::Sender<Value>),
    StopPolling(Fid),
}

pub(crate) struct Poller(UnboundedSender<FileReq>);

impl Poller {
    async fn run(
        updates: mpsc::UnboundedSender<(Fid, Value)>,
        mut req: mpsc::UnboundedReceiver<FileReq>,
    ) -> Result<()> {
        let (clock, _) = broadcast::channel(3);
        let mut polling: FxHashMap<Fid, JoinHandle<Result<()>>> = HashMap::default();
        let mut clock_timer = time::interval(Duration::from_secs(1));
        loop {
            select_biased! {
                _ = clock_timer.tick().fuse() => {
                    let _ = clock.send(());
                }
                r = req.select_next_some() => match r {
                    FileReq::StartPolling(id, path, first) => {
                        let clock = clock.subscribe();
                        let jh = tokio_uring::spawn(
                            poll_file(path, id, updates.clone(), first, clock)
                        );
                        polling.insert(id, jh);
                    }
                    FileReq::StopPolling(id) => {
                        if let Some(jh) = polling.remove(&id) {
                            jh.abort();
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn new(updates: mpsc::UnboundedSender<(Fid, Value)>) -> Self {
        let (tx_req, rx_req) = mpsc::unbounded();
        std::thread::spawn(move || {
            tokio_uring::start(async move {
                if let Err(e) = Self::run(updates, rx_req).await {
                    error!("file poller failed {}", e)
                }
            })
        });
        Self(tx_req)
    }

    pub(crate) async fn start(&self, path: PathBuf) -> Result<(Fid, Value)> {
        let id = Fid::new();
        let (tx, rx) = oneshot::channel();
        match self.0.unbounded_send(FileReq::StartPolling(id, path, tx)) {
            Ok(()) => match rx.await {
                Ok(v) => Ok((id, v)),
                Err(_) => bail!("failed to poll file"),
            },
            Err(_) => bail!("file poller dead"),
        }
    }

    pub(crate) fn stop(&self, id: Fid) -> Result<()> {
        match self.0.unbounded_send(FileReq::StopPolling(id)) {
            Ok(()) => Ok(()),
            Err(_) => bail!("file poller dead"),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum FType {
    File,
    Directory,
}

pub(crate) struct Files {
    pub(crate) paths: BTreeMap<PathBuf, FType>,
    pub(crate) symlinks: BTreeMap<PathBuf, PathBuf>,
}

impl Files {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
        let mut t = Self {
            paths: BTreeMap::default(),
            symlinks: BTreeMap::default(),
        };
        t.walk(path)?;
        Ok(t)
    }

    fn resolve_<'a, 'b: 'a>(
        &'a self,
        target: &'b PathBuf,
        cycle: usize,
    ) -> Result<(&'a PathBuf, FType)> {
        match self.symlinks.get(target) {
            Some(target) => {
                let cycle = cycle + 1;
                if cycle > 256 {
                    bail!("too many levels of symbolic links")
                }
                self.resolve_(target, cycle)
            }
            None => match self.paths.get(target) {
                None => bail!("broken link"),
                Some(ft) => Ok((target, *ft)),
            },
        }
    }

    pub(crate) fn resolve<'a, 'b: 'a>(
        &'a self,
        target: &'b PathBuf,
    ) -> Result<(&'a PathBuf, FType)> {
        self.resolve_(target, 0)
    }

    fn walk(&mut self, path: PathBuf) -> Result<()> {
        use std::fs;
        let st = fs::symlink_metadata(&path)?;
        if st.is_symlink() {
            let target = fs::read_link(&path)?;
            self.symlinks.insert(path, target);
        } else if st.is_dir() {
            for ent in fs::read_dir(&path)? {
                self.walk(ent?.path())?
            }
            self.paths.insert(path, FType::Directory);
        } else if st.is_file() {
            self.paths.insert(path, FType::File);
        }
        Ok(())
    }
}
