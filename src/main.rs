mod fs;
use crate::fs::{
    FType, Fid, FilePoller, Paths, StructureAction, StructureItem, StructurePoller, StructureUpdate,
};
use anyhow::Result;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
    select_biased,
};
use fxhash::FxHashMap;
use log::warn;
use netidx::{
    chars::Chars,
    path::Path,
    pool::Pooled,
    publisher::{
        BindCfg, DefaultHandle, Event, Id, PublishFlags, Publisher, PublisherBuilder, Val, Value,
        WriteRequest,
    },
    utils::{BatchItem, Batched},
};
use netidx_tools_core::ClientParams;
use std::{
    collections::{BTreeMap, HashMap},
    mem,
    ops::{Bound, Deref, DerefMut},
    path::PathBuf,
    time::Duration,
};
use structopt::StructOpt;
use tokio::{
    self,
    time::{self, Instant},
};
use triomphe::Arc;

#[derive(StructOpt, Debug)]
struct Params {
    #[structopt(flatten)]
    common: ClientParams,
    #[structopt(
        short = "b",
        long = "bind",
        help = "configure the bind address",
        default_value = "local"
    )]
    bind: BindCfg,
    #[structopt(
        long = "timeout",
        help = "require subscribers to consume values before timeout (seconds)"
    )]
    timeout: Option<u64>,
    #[structopt(
        long = "netidx-base",
        help = "the base path to publish under",
        default_value = "/local/system/sysfs"
    )]
    netidx_base: Path,
    #[structopt(
        long = "path",
        help = "path to the files you want to publish",
        default_value = "/sys"
    )]
    path: PathBuf,
}

struct PublishedFile {
    fid: Fid,
    path: Path,
    val: Val,
}

#[derive(Debug, PartialEq, Eq)]
enum AType {
    File,
    Directory,
    Symlink { target: Path },
}

#[derive(Debug)]
struct AdvertisedFile {
    fspath: Arc<PathBuf>,
    typ: AType,
    id: Option<Id>,
}

struct Advertised(BTreeMap<Path, AdvertisedFile>);

impl Deref for Advertised {
    type Target = BTreeMap<Path, AdvertisedFile>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Advertised {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Advertised {
    fn new() -> Self {
        Advertised(BTreeMap::default())
    }

    fn resolve_mut(&mut self, path: &Path) -> Option<&mut AdvertisedFile> {
        let target: Option<Path> = self.get(path).and_then(|adf| match adf.typ {
            AType::File | AType::Directory => None,
            AType::Symlink { ref target } => Some(target.clone()),
        });
        match target {
            Some(target) => self.resolve_mut(&target),
            None => self.get_mut(path),
        }
    }

    fn resolve(&self, path: &Path) -> Option<&AdvertisedFile> {
        self.get(path).and_then(|adf| match adf.typ {
            AType::File | AType::Directory => Some(adf),
            AType::Symlink { ref target } => self.resolve(target),
        })
    }

    fn children<'a, 'b: 'a>(
        &'a self,
        base: &'b str,
    ) -> impl Iterator<Item = (&'a Path, &'a AdvertisedFile)> {
        self.range::<str, (Bound<&str>, Bound<&str>)>((Bound::Included(base), Bound::Unbounded))
            .take_while(|(p, _)| Path::is_parent(base, p))
    }

    fn is_leaf_dir<P: AsRef<str>>(&self, path: P) -> bool {
        let mut ch = self.children(path.as_ref());
        match ch.next() {
            None => false,
            Some((
                _,
                AdvertisedFile {
                    typ: AType::Directory,
                    ..
                },
            )) => ch.next().is_none(),
            Some(_) => false,
        }
    }
}

fn advertise(dp: &DefaultHandle, path: Path) {
    if let Err(e) = dp.advertise(path.clone()) {
        warn!("failed to advertise path {}, {}", path, e)
    }
}

struct Published {
    published: FxHashMap<Id, PublishedFile>,
    by_fid: FxHashMap<Fid, Id>,
    advertised: Advertised,
    used: FxHashMap<Path, Instant>,
    paths: Paths,
}

impl Published {
    fn new(paths: Paths) -> Self {
        Self {
            published: HashMap::default(),
            by_fid: HashMap::default(),
            advertised: Advertised::new(),
            used: HashMap::default(),
            paths,
        }
    }

    fn remove(&mut self, dp: &DefaultHandle, path: &Path) {
        if let Some(AdvertisedFile { id: Some(id), .. }) = self.advertised.remove(path) {
            if let Some(pf) = self.published.remove(&id) {
                self.by_fid.remove(&pf.fid);
            }
        }
        self.maybe_add_parent_dir_advertisement(dp, path)
    }

    fn maybe_add_parent_dir_advertisement(&mut self, dp: &DefaultHandle, path: &Path) {
        if let Some(parent) = Path::dirname(path) {
            if self.advertised.is_leaf_dir(parent) {
                advertise(dp, Path::from_str(parent));
            }
        }
    }

    // we advertise directories as values only if they have no children
    fn maybe_remove_parent_dir_advertisement(&mut self, dp: &DefaultHandle, path: &Path) {
        if let Some(parent) = Path::dirname(&path) {
            if !self.advertised.is_leaf_dir(parent) {
                if let Some((p, _)) = self.advertised.get_key_value(parent) {
                    dp.remove_advertisement(p);
                }
            }
        }
    }

    fn netidx_paths<T: AsRef<std::path::Path>, U: AsRef<std::path::Path>>(
        &self,
        src: T,
        tgt: U,
    ) -> Option<(Path, Path)> {
        Some((
            self.paths.netidx_path(src.as_ref())?,
            self.paths.netidx_path(tgt.as_ref())?,
        ))
    }

    fn advertise(&mut self, dp: &DefaultHandle, update: StructureUpdate) {
        for up in update.changes {
            match &up.item {
                StructureItem::Directory => {
                    if let Some(path) = self.paths.netidx_path(&*up.path) {
                        match up.action {
                            StructureAction::Removed => self.remove(dp, &path),
                            StructureAction::Added => {
                                let adf = AdvertisedFile {
                                    fspath: up.path.clone(),
                                    typ: AType::Directory,
                                    id: None,
                                };
                                self.advertised.insert(path.clone(), adf);
                                if self.advertised.is_leaf_dir(&path) {
                                    advertise(dp, path.clone());
                                }
                            }
                        }
                    }
                }
                StructureItem::File => {
                    if let Some(path) = self.paths.netidx_path(&*up.path) {
                        match up.action {
                            StructureAction::Removed => self.remove(dp, &path),
                            StructureAction::Added => {
                                advertise(dp, path.clone());
                                let adf = AdvertisedFile {
                                    fspath: up.path.clone(),
                                    typ: AType::File,
                                    id: None,
                                };
                                self.advertised.insert(path.clone(), adf);
                                self.maybe_remove_parent_dir_advertisement(dp, &path);
                            }
                        }
                    }
                }
                StructureItem::Symlink { target } => match up.action {
                    StructureAction::Removed => match update.previous.resolve(&target) {
                        Err(e) => warn!("bad symlink {:?} -> {:?} {}", &up.path, &target, e),
                        Ok((target, typ)) => match typ {
                            FType::File => {
                                let src = match self.paths.netidx_path(&*up.path) {
                                    Some(p) => p,
                                    None => continue,
                                };
                                dp.remove_advertisement(&src);
                                self.advertised.remove(&src);
                                self.maybe_add_parent_dir_advertisement(dp, &src);
                            }
                            FType::Directory => {
                                let (src_path, _) = match self.netidx_paths(&*up.path, target) {
                                    Some((s, t)) => (s, t),
                                    None => continue,
                                };
                                dp.remove_advertisement(&src_path);
                                update.previous.iter_children_no_pfx(target, |p, _| {
                                    let p = p.as_os_str().to_string_lossy();
                                    let src_path = src_path.append(&p);
                                    dp.remove_advertisement(&src_path);
                                    self.advertised.remove(&src_path);
                                });
                                self.maybe_add_parent_dir_advertisement(dp, &src_path);
                            }
                        },
                    },
                    StructureAction::Added => match update.current.resolve(&target) {
                        Err(e) => warn!("bad symlink {:?} -> {:?} {}", &up.path, &target, e),
                        Ok((target, typ)) => match typ {
                            FType::File => {
                                let (source, target) = match self.netidx_paths(&*up.path, &*target)
                                {
                                    Some((s, t)) => (s, t),
                                    None => return,
                                };
                                advertise(dp, source.clone());
                                let adf = AdvertisedFile {
                                    fspath: up.path.clone(),
                                    typ: AType::Symlink { target },
                                    id: None,
                                };
                                self.advertised.insert(source.clone(), adf);
                                self.maybe_remove_parent_dir_advertisement(dp, &source);
                            }
                            FType::Directory => {
                                let (src_path, tgt_path) =
                                    match self.netidx_paths(&*up.path, target) {
                                        Some((s, t)) => (s, t),
                                        None => continue,
                                    };
                                let adf = AdvertisedFile {
                                    fspath: up.path.clone(),
                                    typ: AType::Directory,
                                    id: None,
                                };
                                self.advertised.insert(src_path.clone(), adf);
                                update.current.iter_children_no_pfx(target, |p, _| {
                                    let s = p.as_os_str().to_string_lossy();
                                    let src_path = src_path.append(&s);
                                    let tgt_path = tgt_path.append(&s);
                                    advertise(dp, src_path.clone());
                                    let adf = AdvertisedFile {
                                        fspath: Arc::new(up.path.join(p)),
                                        typ: AType::Symlink { target: tgt_path },
                                        id: None,
                                    };
                                    self.advertised.insert(src_path, adf);
                                });
                                if self.advertised.is_leaf_dir(&src_path) {
                                    advertise(dp, src_path.clone());
                                }
                                self.maybe_remove_parent_dir_advertisement(dp, &src_path);
                            }
                        },
                    },
                },
            }
        }
    }

    fn used_file(&mut self, path: &Path) {
        let base = Path::dirname(&path).unwrap_or_else(|| "/");
        match self.used.get_mut(base) {
            Some(i) => {
                *i = Instant::now();
            }
            None => {
                self.used.insert(Path::from_str(base), Instant::now());
            }
        }
    }

    fn used_directory(&mut self, path: &Path) {
        *self.used.entry(path.clone()).or_insert_with(Instant::now) = Instant::now();
    }

    fn gc_structure(&mut self, structure_poller: &mut StructurePoller) {
        const TIMEOUT: Duration = Duration::from_secs(60);
        let mut to_remove: Vec<Path> = Vec::new();
        let used = &mut self.used;
        let advertised = &mut self.advertised;
        used.retain(|path, last| {
            last.elapsed() < TIMEOUT || {
                let published =
                    advertised
                        .children(path)
                        .any(|(path, _)| match advertised.resolve(path) {
                            Some(adf) => adf.id.is_some(),
                            None => false,
                        });
                published || {
                    to_remove.push(path.clone());
                    false
                }
            }
        });
        for path in to_remove {
            if let Some(fspath) = self.paths.fs_path(&path) {
                if let Err(e) = structure_poller.stop_by_path(Arc::new(fspath)) {
                    warn!("failed to stop polling directory {}, {}", path, e)
                }
            }
        }
    }
}

async fn handle_subscribe_advertised(
    published: &mut Published,
    publisher: &Publisher,
    writes: &mpsc::Sender<Pooled<Vec<WriteRequest>>>,
    file_poller: &FilePoller,
    path: &Path,
    reply: oneshot::Sender<()>,
) {
    if let Some(adf) = published.advertised.resolve_mut(path) {
        match adf.typ {
            AType::Symlink { .. } => unreachable!(), // resolve should never return a symlink
            AType::Directory => published.used_directory(path),
            AType::File => {
                match adf.id {
                    Some(id) => match publisher.alias(id, path.clone()) {
                        Ok(()) => (),
                        Err(e) => warn!("failed to alias {}, {}", path, e),
                    },
                    None => match file_poller.start(adf.fspath.clone()).await {
                        Err(e) => warn!("polling file {} failed {}", path, e),
                        Ok((fid, v)) => {
                            let flags = PublishFlags::DESTROY_ON_IDLE;
                            match publisher.publish_with_flags(flags, path.clone(), v) {
                                Err(e) => warn!("failed to publish {}, {}", path, e),
                                Ok(val) => {
                                    let vid = val.id();
                                    adf.id = Some(vid);
                                    let pf = PublishedFile {
                                        fid,
                                        path: path.clone(),
                                        val,
                                    };
                                    published.published.insert(vid, pf);
                                    published.by_fid.insert(fid, vid);
                                    publisher.writes(vid, writes.clone());
                                    let _ = reply.send(());
                                }
                            }
                        }
                    },
                }
                published.used_file(path);
            }
        }
    }
}

async fn handle_writes(
    poller: &FilePoller,
    published: &Published,
    mut reqs: Pooled<Vec<WriteRequest>>,
) {
    macro_rules! send_result {
        ($req:expr, $v:expr) => {
            if let Some(send) = $req.send_result {
                send.send($v);
            }
        };
    }
    futures::future::join_all(reqs.drain(..).map(|req| async move {
        match published.advertised.resolve(&req.path) {
            None => send_result!(req, Value::Error(Chars::from("no such file"))),
            Some(AdvertisedFile {
                fspath: _,
                typ: AType::File,
                id: Some(id),
            }) => match published.published.get(&id) {
                None => send_result!(req, Value::Error(Chars::from("no such file"))),
                Some(p) => match poller.write(p.fid, req.value).await {
                    Ok(()) => send_result!(req, Value::Ok),
                    Err(e) => send_result!(req, Value::Error(Chars::from(format!("{}", e)))),
                },
            },
            Some(_) => send_result!(req, Value::Error(Chars::from("not a file"))),
        }
    }))
    .await;
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opts = Params::from_args();
    let (cfg, auth) = opts.common.load();
    let timeout = opts.timeout.map(Duration::from_secs);
    let publisher = PublisherBuilder::new(cfg)
        .desired_auth(auth)
        .bind_cfg(Some(opts.bind))
        .build()
        .await?;
    let (tx_file_updates, rx_file_updates) = mpsc::unbounded();
    let (tx_structure_updates, mut rx_structure_updates) = mpsc::unbounded();
    let (tx_events, mut rx_events) = mpsc::unbounded();
    let (tx_writes, mut rx_writes) = mpsc::channel(50);
    publisher.events(tx_events);
    let mut rx_file_updates = Batched::new(rx_file_updates, 10_000);
    let sysfs = Arc::new(opts.path);
    let file_poller = FilePoller::new(tx_file_updates);
    let mut gc = time::interval(Duration::from_secs(10));
    let mut structure_poller = StructurePoller::new(sysfs.clone(), tx_structure_updates);
    let mut dp = publisher.publish_default(opts.netidx_base.clone())?;
    let mut published = Published::new(Paths::new(sysfs.clone(), opts.netidx_base.clone()));
    let mut updates = publisher.start_batch();
    loop {
        select_biased! {
            _ = gc.tick().fuse() => published.gc_structure(&mut structure_poller),
            up = rx_structure_updates.select_next_some() => published.advertise(&dp, up),
            reqs = rx_writes.select_next_some() => handle_writes(&file_poller, &published, reqs).await,
            (p, reply) = dp.select_next_some() => {
                match published.advertised.resolve(&p) {
                    Some(AdvertisedFile { typ: AType::Symlink { .. }, .. }) => unreachable!(),
                    Some(AdvertisedFile { typ: AType::File, .. }) => {
                        handle_subscribe_advertised(
                            &mut published,
                            &publisher,
                            &tx_writes,
                            &file_poller,
                            &p,
                            reply
                        ).await
                    },
                    r@ (None | Some(AdvertisedFile {typ: AType::Directory, ..})) => {
                        let dir = r.is_some();
                        if let Some(mut fspath) = published.paths.fs_path(&p) {
                            let fspath = if dir {
                                Arc::new(fspath)
                            } else {
                                fspath.pop();
                                if fspath.starts_with(&*sysfs) {
                                    Arc::new(fspath)
                                } else {
                                    sysfs.clone()
                                }
                            };
                            match structure_poller.start(fspath).await {
                                Ok(None) => (), // already polling this
                                Err(e) => warn!("failed to poll {}", e),
                                Ok(Some(up)) => {
                                    published.advertise(&dp, up);
                                    handle_subscribe_advertised(
                                        &mut published,
                                        &publisher,
                                        &tx_writes,
                                        &file_poller,
                                        &p,
                                        reply
                                    ).await
                                },
                            }
                        }
                    }
                }
            },
            r = rx_file_updates.select_next_some() => match r {
                BatchItem::InBatch((fid, v)) => match published.by_fid.get(&fid) {
                    None => file_poller.stop(fid)?,
                    Some(id) => match published.published.get(id) {
                        None => file_poller.stop(fid)?,
                        Some(pv) => pv.val.update_changed(&mut updates, v),
                    }
                }
                BatchItem::EndBatch => {
                    let up = mem::replace(&mut updates, publisher.start_batch());
                    up.commit(timeout).await
                }
            },
            e = rx_events.select_next_some() => match e {
                Event::Subscribe(_, _) | Event::Unsubscribe(_, _) => (),
                Event::Destroyed(id) => {
                    if let Some(pv) = published.published.remove(&id) {
                        if let Err(e) = file_poller.stop(pv.fid) {
                            warn!("failed to stop polling {:?}, {}", pv.path, e);
                        }
                        if let Some(adf) = published.advertised.resolve_mut(&pv.path) {
                            adf.id = None;
                        }
                    }
                }
            }
        }
    }
}
