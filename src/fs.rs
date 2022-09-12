use anyhow::{bail, Result};
use bytes::Bytes;
use futures::{
    channel::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    prelude::*,
    select_biased,
};
use fxhash::FxHashMap;
use immutable_chunkmap::map::MapS as Map;
use log::{error, info, warn};
use netidx::{path::Path as NPath, publisher::Value};
use std::{
    cmp::min, collections::HashMap, ops::Bound, os::linux::fs::MetadataExt, path::PathBuf,
    time::Duration,
};
use tokio::{sync::broadcast, task, time};
use tokio_uring::{
    self,
    buf::IoBuf,
    fs::{File, OpenOptions},
};
use triomphe::Arc;

pub(crate) struct Paths {
    pub(crate) base: Arc<PathBuf>,
    pub(crate) netidx_base: NPath,
}

impl Paths {
    pub(crate) fn new(base: Arc<PathBuf>, netidx_base: NPath) -> Self {
        Self { base, netidx_base }
    }

    pub(crate) fn netidx_path<P: AsRef<std::path::Path>>(&self, path: P) -> Option<NPath> {
        let path = path.as_ref().strip_prefix(&*self.base).ok()?;
        Some(self.netidx_base.append(&path.as_os_str().to_string_lossy()))
    }

    pub(crate) fn fs_path(&self, path: &NPath) -> Option<PathBuf> {
        let path = NPath::strip_prefix(&self.netidx_base, path)?;
        Some(self.base.join(PathBuf::from(path)))
    }
}

#[derive(Debug)]
pub(crate) enum StructureItem {
    File,
    Directory,
    Symlink { target: Arc<PathBuf> },
}

impl StructureItem {
    fn from_ftype(ft: &FType) -> Self {
        match ft {
            FType::File => Self::File,
            FType::Directory => Self::Directory,
        }
    }
}

#[derive(Debug)]
pub(crate) enum StructureAction {
    Added,
    Removed,
}

#[derive(Debug)]
pub(crate) struct StructureItemUpdate {
    pub(crate) path: Arc<PathBuf>,
    pub(crate) action: StructureAction,
    pub(crate) item: StructureItem,
}

#[derive(Debug)]
pub(crate) struct StructureUpdate {
    pub(crate) current: Files,
    pub(crate) previous: Files,
    pub(crate) changes: Vec<StructureItemUpdate>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum FType {
    File,
    Directory,
}

#[derive(Debug, Clone)]
pub(crate) struct Files {
    pub(crate) paths: Map<Arc<PathBuf>, FType>,
    pub(crate) symlinks: Map<Arc<PathBuf>, Arc<PathBuf>>,
}

impl Files {
    pub(crate) fn empty() -> Self {
        Self {
            paths: Map::default(),
            symlinks: Map::default(),
        }
    }

    pub(crate) fn iter_children_no_pfx<F: FnMut(&std::path::Path, &FType)>(
        &self,
        base: &PathBuf,
        mut f: F,
    ) {
        let children = self
            .paths
            .range(Bound::Excluded(Arc::new(base.clone())), Bound::Unbounded)
            .take_while(|(p, _)| p.starts_with(base))
            .filter_map(|(p, t)| p.strip_prefix(base).ok().map(|p| (p, t)));
        for (p, t) in children {
            f(p, t)
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

    pub(crate) fn diff(&self, other: &Self) -> Vec<StructureItemUpdate> {
        let mut changes: Vec<StructureItemUpdate> = Vec::new();
        let removed = self.paths.diff(&other.paths, |_, _, _| None);
        let added = other.paths.diff(&self.paths, |_, _, _| None);
        let removed_links = self.symlinks.diff(&other.symlinks, |_, _, _| None);
        let added_links = other.symlinks.diff(&self.symlinks, |_, _, _| None);
        for (path, ftype) in &removed {
            changes.push(StructureItemUpdate {
                path: path.clone(),
                action: StructureAction::Removed,
                item: StructureItem::from_ftype(ftype),
            });
        }
        for (path, ftype) in &added {
            changes.push(StructureItemUpdate {
                path: path.clone(),
                action: StructureAction::Added,
                item: StructureItem::from_ftype(ftype),
            })
        }
        for (path, target) in &added_links {
            changes.push(StructureItemUpdate {
                path: path.clone(),
                action: StructureAction::Added,
                item: StructureItem::Symlink {
                    target: target.clone(),
                },
            })
        }
        for (path, target) in &removed_links {
            changes.push(StructureItemUpdate {
                path: path.clone(),
                action: StructureAction::Removed,
                item: StructureItem::Symlink {
                    target: target.clone(),
                },
            })
        }
        changes
    }

    pub(crate) fn walk(&self, base: &Arc<PathBuf>, path: Arc<PathBuf>, max_depth: usize) -> Self {
        self.walk_(base, path, max_depth, 0)
    }

    fn walk_(
        &self,
        base: &Arc<PathBuf>,
        path: Arc<PathBuf>,
        max_depth: usize,
        depth: usize,
    ) -> Self {
        if !path.starts_with(&**base) {
            return self.clone();
        }
        const S_IFMT: u32 = 61440;
        const S_IFREG: u32 = 32768;
        macro_rules! log {
            ($map:expr, $path:expr, $msg:expr, $e:expr) => {
                match $e {
                    Ok(r) => r,
                    Err(e) => {
                        warn!($msg, $path, e);
                        return $map.clone();
                    }
                }
            };
        }
        use std::fs;
        let st = log!(self, &path, "stat {:?}, {}", fs::symlink_metadata(&*path));
        if st.is_symlink() {
            let target = log!(
                self,
                &path,
                "canonicalize {:?}, {}",
                fs::canonicalize(&*path)
            );
            if !target.starts_with(&**base) {
                self.clone()
            } else {
                let target = Arc::new(target);
                let symlinks = self.symlinks.insert(path, target.clone()).0;
                let t = Self {
                    symlinks,
                    ..self.clone()
                };
                t.walk_(base, target, max_depth, depth)
            }
        } else if st.is_dir() {
            let mut t = self.clone();
            if depth < max_depth {
                for ent in log!(self, &path, "readdir {:?}, {}", fs::read_dir(&*path)) {
                    let ent = log!(self, &path, "reading dir ent in {:?}, {}", ent);
                    t = t.walk_(base, Arc::new(ent.path()), max_depth, depth + 1);
                }
            }
            let paths = t.paths.insert(path, FType::Directory).0;
            Self { paths, ..t }
        } else if st.is_file() {
            if st.st_mode() & S_IFMT == S_IFREG {
                let paths = self.paths.insert(path, FType::File).0;
                Self {
                    paths,
                    ..self.clone()
                }
            } else {
                self.clone()
            }
        } else {
            self.clone()
        }
    }
}

const SZ: usize = 64;

async fn read_file(file: &PathBuf, mut buf: Vec<u8>) -> Result<(usize, Vec<u8>)> {
    const MAX: usize = 512;
    let fd = File::open(file).await?;
    let mut pos = 0;
    let res = loop {
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
    };
    let _ = fd.close().await;
    res
}

async fn write_file(file: &PathBuf, val: Value) -> Result<()> {
    let mut buf = val.to_string_naked().into_bytes();
    let fd = OpenOptions::new().write(true).open(file).await?;
    let mut pos: usize = 0;
    while pos < buf.len() {
        let (written, res) = fd.write_at(buf.slice(pos..), pos as u64).await;
        pos += written?;
        buf = res.into_inner();
    }
    let _ = fd.close().await;
    Ok(())
}

enum PollFileReq {
    Stop,
    Write(Value, oneshot::Sender<Result<()>>),
}

async fn poll_file(
    file: Arc<PathBuf>,
    id: Fid,
    updates: UnboundedSender<(Fid, Value)>,
    first: oneshot::Sender<Value>,
    mut clock: broadcast::Receiver<()>,
    mut input: mpsc::UnboundedReceiver<PollFileReq>,
) -> Result<()> {
    const MAX_SKIP: u16 = 300;
    const WAIT: Duration = Duration::from_secs(2);
    let mut prev = vec![0; SZ];
    let mut buf = vec![0; SZ];
    let mut first = Some(first);
    let send = |first: &mut Option<oneshot::Sender<Value>>, v: Value| -> Result<()> {
        match first.take() {
            None => Ok(updates.unbounded_send((id, v))?),
            Some(first) => {
                let _ = first.send(v);
                Ok(())
            }
        }
    };
    let mut skip: u16 = 0;
    'main: loop {
        let (pos, buf_) = time::timeout(WAIT, read_file(&file, buf)).await??;
        buf = buf_;
        if pos == 0 && first.is_some() {
            send(&mut first, Value::Null)?;
        } else if prev.len() >= pos && &buf[0..pos] == &prev[0..pos] {
            // backoff polling up to max_skip clocks if we find the
            // file's contents unchanged
            skip = min(MAX_SKIP, skip + 1);
        } else {
            skip >>= 1;
            if prev.len() != buf.len() {
                prev.resize(buf.len(), 0);
            }
            prev.copy_from_slice(&buf);
            match std::str::from_utf8(&buf[0..pos]) {
                Err(_) => send(
                    &mut first,
                    Value::from(Bytes::copy_from_slice(&buf[0..pos])),
                )?,
                Ok(data) => match data.trim().parse::<i64>() {
                    Ok(i) => send(&mut first, Value::from(i))?,
                    Err(_) => match data.trim().parse::<f64>() {
                        Ok(f) => send(&mut first, Value::from(f))?,
                        Err(_) => send(&mut first, Value::from(String::from(data.trim())))?,
                    },
                },
            }
        }
        let mut skipped: u16 = 0;
        let mut write: Option<(Value, oneshot::Sender<Result<()>>)> = None;
        loop {
            select_biased! {
                m = input.select_next_some() => match m {
                    PollFileReq::Stop => break 'main Ok(()),
                    PollFileReq::Write(v, reply) => { write = Some((v, reply)); }
                },
                r = clock.recv().fuse() => if let Err(_) = r {
                    break 'main Ok(());
                },
                complete => break 'main Ok(()),
            }
            // we delay writes so they happen on the clock edge along
            // with reads, for best syscall batching with io uring.
            if let Some((v, reply)) = write.take() {
                let r = match time::timeout(WAIT, write_file(&*file, v))
                    .await
                    .map_err(anyhow::Error::from)
                {
                    Err(e) => Err(e),
                    Ok(Err(e)) => Err(e),
                    Ok(Ok(())) => Ok(()),
                };
                let _ = reply.send(r);
            } else {
                skipped += 1;
                if skipped >= skip {
                    break;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) struct Fid(u32);

impl Fid {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static NEXT: AtomicU32 = AtomicU32::new(0);
        Fid(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

enum FileReq {
    Write(Fid, Value, oneshot::Sender<Result<()>>),
    StartPolling(Fid, Arc<PathBuf>, oneshot::Sender<Value>),
    StopPolling(Fid),
}

pub(crate) struct FilePoller(UnboundedSender<FileReq>);

impl FilePoller {
    async fn run(
        updates: UnboundedSender<(Fid, Value)>,
        mut req: UnboundedReceiver<FileReq>,
    ) -> Result<()> {
        let (clock, _) = broadcast::channel(3);
        let mut polling: FxHashMap<Fid, mpsc::UnboundedSender<PollFileReq>> = HashMap::default();
        let mut clock_timer = time::interval(Duration::from_secs(1));
        loop {
            select_biased! {
                _ = clock_timer.tick().fuse() => {
                    let _ = clock.send(());
                }
                r = req.select_next_some() => match r {
                    FileReq::Write(id, v, reply) => if let Some(chan) = polling.get(&id) {
                        let _ = chan.unbounded_send(PollFileReq::Write(v, reply));
                    }
                    FileReq::StartPolling(id, path, first) => {
                        let clock = clock.subscribe();
                        let (tx_req, rx_req) = mpsc::unbounded();
                        let updates = updates.clone();
                        tokio_uring::spawn(async move {
                            let r = poll_file(
                                path.clone(),
                                id,
                                updates,
                                first,
                                clock,
                                rx_req
                            ).await;
                            if let Err(e) = r {
                                warn!("file poll died {:?}, {}", path, e)
                            }
                        });
                        polling.insert(id, tx_req);
                    }
                    FileReq::StopPolling(id) => {
                        if let Some(stop) = polling.remove(&id) {
                            let _ = stop.unbounded_send(PollFileReq::Stop);
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn new(updates: UnboundedSender<(Fid, Value)>) -> Self {
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

    pub(crate) async fn start(&self, path: Arc<PathBuf>) -> Result<(Fid, Value)> {
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

    pub(crate) async fn write(&self, id: Fid, value: Value) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        match self.0.unbounded_send(FileReq::Write(id, value, tx)) {
            Err(_) => bail!("file poller dead"),
            Ok(()) => rx.await?,
        }
    }
}

async fn poll_structure(
    base: Arc<PathBuf>,
    path: Arc<PathBuf>,
    updates: UnboundedSender<StructureUpdate>,
    first: oneshot::Sender<Option<StructureUpdate>>,
    mut stop: oneshot::Receiver<()>,
) {
    const MAX_SKIP: u8 = 120;
    const WAIT: Duration = Duration::from_secs(5);
    let poll = || async {
        let join = task::spawn_blocking({
            let base = base.clone();
            let path = path.clone();
            move || Files::empty().walk(&base, path, 1)
        });
        match time::timeout(WAIT, join).await {
            Ok(Ok(files)) => files,
            Ok(Err(e)) => {
                warn!("failed to join poll task {:?}, {}", path, e);
                Files::empty()
            }
            Err(e) => {
                warn!("timeout polling {:?}, {}", path, e);
                Files::empty()
            }
        }
    };
    let mut files = poll().await;
    let _ = first.send(Some(StructureUpdate {
        current: files.clone(),
        previous: Files::empty(),
        changes: Files::empty().diff(&files),
    }));
    let mut clock = time::interval(Duration::from_secs(1));
    let mut skip: u8 = 0;
    let mut skipped: u8 = 0;
    loop {
        select_biased! {
            _ = clock.tick().fuse() => {
                if skipped < skip {
                    skipped += 1;
                } else {
                    skipped = 0;
                    let current = poll().await;
                    let previous = files.clone();
                    let changes = previous.diff(&current);
                    files = current.clone();
                    if changes.len() == 0 {
                        skip = min(MAX_SKIP, skip + 1);
                    } else {
                        skip >>= 1;
                        let r = updates.unbounded_send(StructureUpdate {
                            current,
                            previous,
                            changes
                        });
                        if let Err(_) = r {
                            break
                        }
                    }
                }
            }
            _ = stop => {
                let previous = files.clone();
                let current = Files::empty();
                let changes = previous.diff(&current);
                let _ = updates.unbounded_send(StructureUpdate {
                    previous,
                    current,
                    changes,
                });
                info!("structure poll of {:?} shutting down as requested", &path);
                break
            },
        }
    }
}

enum StructureReq {
    Start(Arc<PathBuf>, oneshot::Sender<Option<StructureUpdate>>),
    Stop(Arc<PathBuf>),
}

pub(crate) struct StructurePoller(UnboundedSender<StructureReq>);

impl StructurePoller {
    async fn run(
        base: Arc<PathBuf>,
        updates: UnboundedSender<StructureUpdate>,
        mut req: UnboundedReceiver<StructureReq>,
    ) {
        let mut polling: FxHashMap<Arc<PathBuf>, oneshot::Sender<()>> = HashMap::default();
        while let Some(r) = req.next().await {
            match r {
                StructureReq::Stop(path) => {
                    info!("stop polling {:?}", path);
                    if let Some(stop) = polling.remove(&path) {
                        let _ = stop.send(());
                    }
                }
                StructureReq::Start(path, initial) => {
                    if polling.contains_key(&path) {
                        let _ = initial.send(None);
                    } else {
                        info!("start polling {:?}", path);
                        let (tx_stop, rx_stop) = oneshot::channel();
                        polling.insert(path.clone(), tx_stop);
                        task::spawn(poll_structure(
                            base.clone(),
                            path,
                            updates.clone(),
                            initial,
                            rx_stop,
                        ));
                    }
                }
            }
        }
    }

    pub(crate) fn new(base: Arc<PathBuf>, updates: UnboundedSender<StructureUpdate>) -> Self {
        let (tx, rx) = mpsc::unbounded();
        task::spawn(Self::run(base, updates, rx));
        Self(tx)
    }

    pub(crate) async fn start(&mut self, path: Arc<PathBuf>) -> Result<Option<StructureUpdate>> {
        let (tx_init, rx_init) = oneshot::channel();
        let req = StructureReq::Start(path, tx_init);
        if let Err(_) = self.0.unbounded_send(req) {
            bail!("structure poller is dead")
        }
        match rx_init.await {
            Err(_) => bail!("failed to get initial value"),
            Ok(f) => Ok(f),
        }
    }

    pub(crate) fn stop_by_path(&mut self, path: Arc<PathBuf>) -> Result<()> {
        if let Err(_) = self.0.unbounded_send(StructureReq::Stop(path)) {
            bail!("structure poller is dead")
        }
        Ok(())
    }
}
