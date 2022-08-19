//mod config;
mod sysfs;
use crate::sysfs::{FType, Fid, FilePoller, Paths, StructurePoller, StructureUpdate};
use anyhow::Result;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
    select_biased,
};
use fxhash::{FxHashMap, FxHashSet};
use log::warn;
use netidx::{
    path::Path,
    publisher::{BindCfg, DefaultHandle, Event, Id, PublishFlags, Publisher, Val},
    utils::{BatchItem, Batched},
};
use netidx_tools::ClientParams;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    mem,
    ops::Bound,
    path::PathBuf,
    time::Duration,
};
use structopt::StructOpt;
use sysfs::{StructureAction, StructureItem};
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
    #[structopt(long = "base", help = "the base path to publish under")]
    base: Path,
    #[structopt(long = "path", help = "path to the files you want to publish")]
    path: PathBuf,
}

struct PublishedFile {
    fid: Fid,
    path: Path,
    val: Val,
}

#[derive(Debug)]
struct AdvertisedFile {
    fspath: Arc<PathBuf>,
    typ: FType,
    id: Option<Id>,
}

struct Published {
    published: FxHashMap<Id, PublishedFile>,
    by_fid: FxHashMap<Fid, Id>,
    advertised: BTreeMap<Path, AdvertisedFile>,
    aliased: FxHashMap<Path, Path>,
    used: FxHashMap<Path, Instant>,
    paths: Paths,
}

impl Published {
    fn new(paths: Paths) -> Self {
        Self {
            published: HashMap::default(),
            by_fid: HashMap::default(),
            advertised: BTreeMap::default(),
            aliased: HashMap::default(),
            used: HashMap::default(),
            paths,
        }
    }

    fn resolve(&mut self, path: &Path) -> Option<&mut AdvertisedFile> {
        let tgt = self.aliased.get(path).cloned();
        match tgt {
            Some(tgt) => self.resolve(&tgt),
            None => self.advertised.get_mut(path),
        }
    }

    fn remove(&mut self, path: &Path) {
        if let Some(AdvertisedFile { id: Some(id), .. }) = self.advertised.remove(path) {
            if let Some(pf) = self.published.remove(&id) {
                self.by_fid.remove(&pf.fid);
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

    fn add_file_link<T: AsRef<std::path::Path>, U: AsRef<std::path::Path>>(
        &mut self,
        dp: &DefaultHandle,
        src: T,
        tgt: U,
    ) {
        let (src, tgt) = match self.netidx_paths(src, tgt) {
            Some((s, t)) => (s, t),
            None => return,
        };
        if let Err(e) = dp.advertise(src.clone()) {
            warn!("failed to advertise path {}, {}", src, e)
        }
        self.aliased.insert(src, tgt);
    }

    fn remove_file_link(&mut self, dp: &DefaultHandle, src: &Arc<PathBuf>) {
        let src = match self.paths.netidx_path(&**src) {
            Some(p) => p,
            None => return,
        };
        dp.remove_advertisement(&src);
        self.aliased.remove(&src);
    }

    fn advertise(&mut self, dp: &DefaultHandle, update: StructureUpdate) {
        for up in update.changes {
            match &up.item {
                StructureItem::Directory => {
                    if let Some(path) = self.paths.netidx_path(&*up.path) {
                        match up.action {
                            StructureAction::Removed => self.remove(&path),
                            StructureAction::Added => {
                                if let Err(e) = dp.advertise(path.clone()) {
                                    warn!("failed to advertise path {}, {}", path, e)
                                }
                                let adf = AdvertisedFile {
                                    fspath: up.path.clone(),
                                    typ: FType::Directory,
                                    id: None,
                                };
                                self.advertised.insert(path, adf);
                            }
                        }
                    }
                }
                StructureItem::File => {
                    if let Some(path) = self.paths.netidx_path(&*up.path) {
                        match up.action {
                            StructureAction::Removed => self.remove(&path),
                            StructureAction::Added => {
                                if let Err(e) = dp.advertise(path.clone()) {
                                    warn!("failed to advertise path {}, {}", path, e)
                                }
                                let adf = AdvertisedFile {
                                    fspath: up.path.clone(),
                                    typ: FType::File,
                                    id: None,
                                };
                                self.advertised.insert(path, adf);
                            }
                        }
                    }
                }
                StructureItem::Symlink { target } => match up.action {
                    StructureAction::Removed => match update.previous.resolve(&target) {
                        Err(e) => warn!("bad symlink {:?} -> {:?} {}", &up.path, &target, e),
                        Ok((target, typ)) => match typ {
                            FType::File => self.remove_file_link(dp, &up.path),
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
                                    self.aliased.remove(&src_path);
                                })
                            }
                        },
                    },
                    StructureAction::Added => match update.current.resolve(&target) {
                        Err(e) => warn!("bad symlink {:?} -> {:?} {}", &up.path, &target, e),
                        Ok((target, typ)) => match typ {
                            FType::File => self.add_file_link(dp, &*up.path, target),
                            FType::Directory => {
                                let (src_path, tgt_path) =
                                    match self.netidx_paths(&*up.path, target) {
                                        Some((s, t)) => (s, t),
                                        None => continue,
                                    };
                                if let Err(e) = dp.advertise(src_path.clone()) {
                                    warn!("failed to advertise path {}, {}", src_path, e)
                                }
                                let adf = AdvertisedFile {
                                    fspath: up.path.clone(),
                                    typ: FType::Directory,
                                    id: None,
                                };
                                self.advertised.insert(src_path.clone(), adf);
                                update.current.iter_children_no_pfx(target, |p, _| {
                                    let p = p.as_os_str().to_string_lossy();
                                    let src_path = src_path.append(&p);
                                    let tgt_path = tgt_path.append(&p);
                                    if let Err(e) = dp.advertise(src_path.clone()) {
                                        warn!("failed to advertise {}, {}", src_path, e)
                                    }
                                    self.aliased.insert(src_path, tgt_path);
                                })
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
                self.used
                    .insert(Path::from(String::from(base)), Instant::now());
            }
        }
    }

    fn used_directory(&mut self, path: &Path) {
        *self.used.entry(path.clone()).or_insert_with(Instant::now) = Instant::now();
    }

    fn advertised_children<'a, 'b: 'a>(
        advertised: &'a BTreeMap<Path, AdvertisedFile>,
        base: &'b Path,
    ) -> impl Iterator<Item = (&'a Path, &'a AdvertisedFile)> {
        advertised
            .range::<Path, (Bound<&Path>, Bound<&Path>)>((Bound::Included(base), Bound::Unbounded))
            .take_while(|(p, _)| Path::is_parent(base, p))
    }

    fn gc_structure(&mut self, dp: &DefaultHandle, structure_poller: &mut StructurePoller) {
        const TIMEOUT: Duration = Duration::from_secs(60);
        let mut to_remove: FxHashSet<Path> = HashSet::default();
        let mut stopped: FxHashSet<Path> = HashSet::default();
        let used = dbg!(&mut self.used);
        let advertised = &mut self.advertised;
        used.retain(|path, last| {
            last.elapsed() < TIMEOUT || {
                let published =
                    Self::advertised_children(advertised, path).any(|(_, adf)| adf.id.is_some());
                published || {
                    to_remove.extend(
                        Self::advertised_children(advertised, path).map(|(p, _)| p.clone()),
                    );
                    false
                }
            }
        });
        for path in to_remove {
            self.aliased.remove(&path);
            dp.remove_advertisement(&path);
            if let Some(adf) = advertised.remove(&path) {
                let base = match adf.typ {
                    FType::Directory => path.clone(),
                    FType::File => {
                        let base = Path::basename(&path).unwrap_or("/");
                        stopped
                            .get(base)
                            .cloned()
                            .unwrap_or_else(|| Path::from(String::from(base)))
                    }
                };
                if !stopped.contains(&base) {
                    if let Some(fspath) = self.paths.fs_path(&base) {
                        if let Err(e) = structure_poller.stop_by_path(Arc::new(fspath)) {
                            warn!("failed to stop polling directory {}, {}", path, e)
                        }
                        stopped.insert(base);
                    }
                }
            }
        }
    }
}

async fn handle_subscribe_advertised(
    published: &mut Published,
    publisher: &Publisher,
    file_poller: &FilePoller,
    path: &Path,
    reply: oneshot::Sender<()>,
) {
    if let Some(adf) = published.resolve(path) {
        match adf.typ {
            FType::Directory => published.used_directory(path),
            FType::File => {
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

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opts = Params::from_args();
    let (cfg, auth) = opts.common.load();
    let timeout = opts.timeout.map(Duration::from_secs);
    let publisher = Publisher::new(cfg, auth, opts.bind).await?;
    let (tx_file_updates, rx_file_updates) = mpsc::unbounded();
    let (tx_structure_updates, mut rx_structure_updates) = mpsc::unbounded();
    let (tx_events, mut rx_events) = mpsc::unbounded();
    publisher.events(tx_events);
    let mut rx_file_updates = Batched::new(rx_file_updates, 10_000);
    let sysfs = Arc::new(opts.path);
    let file_poller = FilePoller::new(tx_file_updates);
    let mut gc = time::interval(Duration::from_secs(60));
    let mut structure_poller = StructurePoller::new(sysfs.clone(), tx_structure_updates);
    let mut dp = publisher.publish_default(opts.base.clone())?;
    let mut published = Published::new(Paths::new(sysfs.clone(), opts.base.clone()));
    let mut updates = publisher.start_batch();
    loop {
        select_biased! {
            _ = gc.tick().fuse() => published.gc_structure(&dp, &mut structure_poller),
            up = rx_structure_updates.select_next_some() => published.advertise(&dp, up),
            (p, reply) = dp.select_next_some() => {
                match published.resolve(&p) {
                    Some(AdvertisedFile {typ: FType::File, ..}) => {
                        handle_subscribe_advertised(
                            &mut published,
                            &publisher,
                            &file_poller,
                            &p,
                            reply
                        ).await
                    },
                    r@ (None | Some(AdvertisedFile {typ: FType::Directory, ..})) => {
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
                        if let Some(adf) = published.resolve(&pv.path) {
                            adf.id = None;
                        }
                    }
                }
            }
        }
    }
}
