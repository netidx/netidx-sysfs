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
use netidx::{publisher::Value, path::Path as NPath};
use bytes::Bytes;
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    time::Duration,
};
use tokio::{sync::broadcast, time};
use tokio_uring::{self, buf::IoBuf, fs::File};

pub(crate) async fn poll_file(
    file: PathBuf,
    id: Fid,
    updates: UnboundedSender<(Fid, Value)>,
    first: oneshot::Sender<Value>,
    mut clock: broadcast::Receiver<()>,
    mut stop: oneshot::Receiver<()>,
) -> Result<()> {
    const SZ: usize = 64;
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
    loop {
        let pos = {
            let fd = File::open(&file).await?;
            let mut pos = 0;
            loop {
                let len = buf.len();
                if len - pos < SZ {
                    buf.resize(len * 2, 0);
                }
                let (read, res) = fd.read_at(buf.slice(pos..), pos as u64).await;
                let read = read?;
                if read == 0 {
                    buf = res.into_inner();
                    break pos;
                } else {
                    buf = res.into_inner();
                    pos += read;
                }
            }
        };
        match std::str::from_utf8(&buf[0..pos]) {
            Err(_) => send(Value::from(Bytes::copy_from_slice(&buf[0..pos])))?,
            Ok(data) => match data.trim().parse::<i64>() {
                Ok(i) => send(Value::from(i))?,
                Err(_) => match data.trim().parse::<f64>() {
                    Ok(f) => send(Value::from(f))?,
                    Err(_) => send(Value::from(String::from(data.trim())))?,
                },
            }
        }
        select_biased! {
            _ = stop => break Ok(()),
            r = clock.recv().fuse() => if let Err(_) = r {
                break Ok(());
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

#[derive(Clone, Copy)]
pub(crate) enum FType {
    File,
    Directory,
}

pub(crate) struct Files {
    pub(crate) paths: BTreeMap<PathBuf, FType>,
    pub(crate) symlinks: BTreeMap<PathBuf, PathBuf>,
    pub(crate) base: PathBuf,
}

impl Files {
    pub(crate) fn new(base: PathBuf) -> Result<Self> {
        let mut t = Self {
            paths: BTreeMap::default(),
            symlinks: BTreeMap::default(),
            base: base.clone(),
        };
        t.walk(base)?;
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

    pub(crate) fn netidx_path<P: AsRef<std::path::Path>>(&self, base: &NPath, path: P) -> NPath {
        let path = match path.as_ref().strip_prefix(&self.base) {
            Ok(p) => p,
            Err(_) => path.as_ref(),
        };
        base.append(&path.as_os_str().to_string_lossy())
    }

    fn walk(&mut self, path: PathBuf) -> Result<()> {
        use std::fs;
        let st = fs::symlink_metadata(&path)?;
        if st.is_symlink() {
            let target = fs::canonicalize(&path)?;
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
