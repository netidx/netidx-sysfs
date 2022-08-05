use anyhow::{bail, Result};
use bytes::Bytes;
use futures::{
    channel::{
        mpsc::{self, UnboundedSender},
        oneshot,
    },
    prelude::*,
    select_biased,
};
use fxhash::FxHashMap;
use log::{error, warn};
use netidx::{path::Path as NPath, publisher::Value};
use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
    ops::Bound,
    os::linux::fs::MetadataExt,
    path::PathBuf,
    time::Duration,
};
use tokio::{sync::broadcast, time};
use tokio_uring::{self, buf::IoBuf, fs::File};

const SZ: usize = 64;

async fn read_file(file: &PathBuf, mut buf: Vec<u8>) -> Result<(usize, Vec<u8>)> {
    const MAX: usize = 512;
    let fd = File::open(file).await?;
    let mut pos = 0;
    loop {
        let len = buf.len();
        if len - pos < SZ && len < MAX {
            buf.resize(len * 2, 0);
        }
        let (read, res) = fd.read_at(buf.slice(pos..), pos as u64).await;
        let read = read?;
        if read == 0 {
            buf = res.into_inner();
            break Ok((pos, buf));
        } else {
            buf = res.into_inner();
            pos += read;
            if pos >= MAX {
                break Ok((pos, buf)); // we've read as much as we can
            }
        }
    }
}

pub(crate) async fn poll_file(
    file: PathBuf,
    id: Fid,
    updates: UnboundedSender<(Fid, Value)>,
    first: oneshot::Sender<Value>,
    mut clock: broadcast::Receiver<()>,
    mut stop: oneshot::Receiver<()>,
) -> Result<()> {
    const MAX_SKIP: usize = 300;
    let mut prev = vec![0; SZ];
    let mut buf = vec![0; SZ];
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
    let mut skip = 0;
    'main: loop {
        let (pos, buf_) = read_file(&file, buf).await?;
        buf = buf_;
        if prev.len() >= pos && &buf[0..pos] == &prev[0..pos] {
            // backoff polling up to max_skip clocks if we find the
            // file's contents unchanged
            skip = max(MAX_SKIP, skip + 1);
        } else {
            if prev.len() != buf.len() {
                prev.resize(buf.len(), 0);
            }
            prev.copy_from_slice(&buf);
            match std::str::from_utf8(&buf[0..pos]) {
                Err(_) => send(Value::from(Bytes::copy_from_slice(&buf[0..pos])))?,
                Ok(data) => match data.trim().parse::<i64>() {
                    Ok(i) => send(Value::from(i))?,
                    Err(_) => match data.trim().parse::<f64>() {
                        Ok(f) => send(Value::from(f))?,
                        Err(_) => send(Value::from(String::from(data.trim())))?,
                    },
                },
            }
        }
        let mut skipped = 0;
        loop {
            select_biased! {
                _ = stop => break 'main Ok(()),
                r = clock.recv().fuse() => if let Err(_) = r {
                    break 'main Ok(());
                }
            }
            skipped += 1;
            if skipped >= skip {
                break;
            }
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
        let mut polling: FxHashMap<Fid, oneshot::Sender<()>> = HashMap::default();
        let mut clock_timer = time::interval(Duration::from_secs(1));
        loop {
            select_biased! {
                _ = clock_timer.tick().fuse() => {
                    let _ = clock.send(());
                }
                r = req.select_next_some() => match r {
                    FileReq::StartPolling(id, path, first) => {
                        let clock = clock.subscribe();
                        let (tx_stop, rx_stop) = oneshot::channel();
                        tokio_uring::spawn(
                            poll_file(path, id, updates.clone(), first, clock, rx_stop)
                        );
                        polling.insert(id, tx_stop);
                    }
                    FileReq::StopPolling(id) => {
                        if let Some(stop) = polling.remove(&id) {
                            let _ = stop.send(());
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

#[derive(Debug, Clone, Copy)]
pub(crate) enum FType {
    File,
    Directory,
}

pub(crate) struct Files {
    pub(crate) paths: BTreeMap<PathBuf, FType>,
    pub(crate) symlinks: BTreeMap<PathBuf, PathBuf>,
    pub(crate) base: PathBuf,
    pub(crate) netidx_base: NPath,
}

impl Files {
    pub(crate) fn new(base: PathBuf, netidx_base: NPath) -> Self {
        Self {
            paths: BTreeMap::default(),
            symlinks: BTreeMap::default(),
            base: base.clone(),
            netidx_base,
        }
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

    pub(crate) fn netidx_path<P: AsRef<std::path::Path>>(&self, path: P) -> Option<NPath> {
        let path = path.as_ref().strip_prefix(&self.base).ok()?;
        Some(self.netidx_base.append(&path.as_os_str().to_string_lossy()))
    }

    pub(crate) fn fs_path(&self, path: &NPath) -> Option<PathBuf> {
        let path = NPath::strip_prefix(&self.netidx_base, path)?;
        Some(self.base.join(PathBuf::from(path)))
    }

    fn children<T>(tree: &BTreeMap<PathBuf, T>, base: &PathBuf) -> Vec<PathBuf> {
        use std::path::Path;
        tree
            .range::<Path, (Bound<&Path>, Bound<&Path>)>((Bound::Included(base), Bound::Unbounded))
            .take_while(|(p, _)| p.starts_with(base))
            .map(|(p, _)| p.clone())
            .collect::<Vec<_>>()
    }
    
    pub(crate) fn remove_children(&mut self, base: &PathBuf) {
        for path in Self::children(&self.paths, base) {
            self.paths.remove(&path);
        }
        for path in Self::children(&self.symlinks, base) {
            self.symlinks.remove(&path);
        }
    }

    pub(crate) fn walk(&mut self, path: PathBuf, max_depth: usize) {
        self.walk_(path, max_depth, 0)
    }

    fn walk_(&mut self, path: PathBuf, max_depth: usize, depth: usize) {
        const S_IFMT: u32 = 61440;
        const S_IFREG: u32 = 32768;
        if depth >= max_depth {
            return;
        }
        macro_rules! log {
            ($path:expr, $msg:expr, $e:expr) => {
                match $e {
                    Ok(r) => r,
                    Err(e) => {
                        warn!($msg, $path, e);
                        return;
                    }
                }
            };
        }
        use std::fs;
        let st = log!(&path, "stat {:?}, {}", fs::symlink_metadata(&path));
        if st.len() > 1048576 {
            warn!("skipping file {:?} too large {}", &path, st.len())
        } else if st.is_symlink() {
            let target = log!(&path, "canonicalize {:?}, {}", fs::canonicalize(&path));
            if target.starts_with(&self.base) {
                self.symlinks.insert(path, target);
            }
        } else if st.is_dir() {
            for ent in log!(&path, "readdir {:?}, {}", fs::read_dir(&path)) {
                let ent = log!(&path, "reading dir ent in {:?}, {}", ent);
                self.walk_(ent.path(), max_depth, depth + 1)
            }
            self.paths.insert(path, FType::Directory);
        } else if st.is_file() {
            if st.st_mode() & S_IFMT == S_IFREG {
                self.paths.insert(path, FType::File);
            }
        }
    }
}
