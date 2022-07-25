use anyhow::{bail, Result};
use fxhash::FxHashMap;
use log::error;
use netidx::{
    pool::{Pool, Pooled},
    publisher::{Id, Value},
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    sync::mpsc::{self, UnboundedSender},
    task::JoinHandle,
    time,
};
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
    id: Id,
    updates: UnboundedSender<(Id, Value)>,
) -> Result<()> {
    loop {
        let data = read_file(&file).await?;
        let data = String::from_utf8_lossy(&**data);
        match data.parse::<i64>() {
            Ok(i) => updates.send((id, Value::from(i)))?,
            Err(_) => match data.parse::<f64>() {
                Ok(f) => updates.send((id, Value::from(f)))?,
                Err(_) => updates.send((id, Value::from(data.into_owned())))?,
            },
        }
        time::sleep(Duration::from_secs(1)).await
    }
}

enum FileReq {
    StartPolling(Id, PathBuf),
    StopPolling(Id),
}

pub(crate) struct Poller(mpsc::UnboundedSender<FileReq>);

impl Poller {
    async fn run(
        updates: mpsc::UnboundedSender<(Id, Value)>,
        mut req: mpsc::UnboundedReceiver<FileReq>,
    ) -> Result<()> {
        let mut polling: FxHashMap<Id, JoinHandle<Result<()>>> = HashMap::default();
        while let Some(req) = req.recv().await {
            match req {
                FileReq::StartPolling(id, path) => {
                    let jh = tokio_uring::spawn(poll_file(path, id, updates.clone()));
                    polling.insert(id, jh);
                }
                FileReq::StopPolling(id) => {
                    if let Some(jh) = polling.remove(&id) {
                        jh.abort();
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn new(updates: mpsc::UnboundedSender<(Id, Value)>) -> Self {
        let (tx_req, rx_req) = mpsc::unbounded_channel();
        std::thread::spawn(move || {
            tokio_uring::start(async move {
                if let Err(e) = Self::run(updates, rx_req).await {
                    error!("file poller failed {}", e)
                }
            })
        });
        Self(tx_req)
    }

    pub(crate) fn start(&self, id: Id, path: PathBuf) -> Result<()> {
        match self.0.send(FileReq::StartPolling(id, path)) {
            Ok(()) => Ok(()),
            Err(_) => bail!("file poller dead"),
        }
    }

    pub(crate) fn stop(&self, id: Id) -> Result<()> {
        match self.0.send(FileReq::StopPolling(id)) {
            Ok(()) => Ok(()),
            Err(_) => bail!("file poller dead"),
        }
    }
}

pub(crate) struct Files {
    paths: BTreeSet<PathBuf>,
    symlinks: BTreeMap<PathBuf, PathBuf>,
}

impl Files {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
        let mut t = Self {
            paths: BTreeSet::default(),
            symlinks: BTreeMap::default()
        };
        t.walk(path)?;
        Ok(t)
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
            self.paths.insert(path);
        } else if st.is_file() {
            self.paths.insert(path);
        }
        Ok(())
    }
}
