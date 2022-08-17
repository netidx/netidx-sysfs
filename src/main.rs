#[macro_use]
extern crate serde_derive;

//mod config;
mod sysfs;
use crate::sysfs::{FType, Fid, FilePoller, Files, Paths, StructurePoller, StructureUpdate};
use anyhow::Result;
use futures::{channel::mpsc, prelude::*, select_biased};
use fxhash::FxHashMap;
use log::warn;
use netidx::{
    path::Path,
    publisher::{BindCfg, DefaultHandle, Event, Id, PublishFlags, Publisher, Val},
    utils::{BatchItem, Batched},
};
use netidx_tools::ClientParams;
use std::{
    collections::{BTreeMap, HashMap},
    mem,
    ops::Bound,
    path::PathBuf,
    time::{Duration, Instant},
};
use structopt::StructOpt;
use sysfs::{StructureAction, StructureItem};
use tokio;
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
    #[structopt(long = "sysfs", help = "glob pattern of files to publish, multiple")]
    sysfs: PathBuf,
}

struct PublishedFile {
    fid: Fid,
    path: Path,
    val: Val,
}

struct Published {
    published: FxHashMap<Id, PublishedFile>,
    by_fid: FxHashMap<Fid, Id>,
    advertised: FxHashMap<Path, (Arc<PathBuf>, Option<Id>)>,
    aliased: FxHashMap<Path, Path>,
    paths: Paths,
}

impl Published {
    fn new(paths: Paths) -> Self {
        Self {
            published: HashMap::default(),
            by_fid: HashMap::default(),
            advertised: HashMap::default(),
            aliased: HashMap::default(),
            paths,
        }
    }

    fn resolve(&mut self, path: &Path) -> Option<&mut (Arc<PathBuf>, Option<Id>)> {
        let tgt = self.aliased.get(path).cloned();
        match tgt {
            Some(tgt) => self.resolve(&tgt),
            None => self.advertised.get_mut(path),
        }
    }

    fn remove(&mut self, path: &Path) {
        if let Some((_, Some(id))) = self.advertised.remove(path) {
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
                StructureItem::Directory => (),
                StructureItem::File => {
                    if let Some(path) = self.paths.netidx_path(&*up.path) {
                        match up.action {
                            StructureAction::Removed => self.remove(&path),
                            StructureAction::Added => {
                                if let Err(e) = dp.advertise(path.clone()) {
                                    warn!("failed to advertise path {}, {}", path, e)
                                }
                                self.advertised.insert(path, (up.path.clone(), None));
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
                                update.previous.iter_children_no_pfx(target, |p, typ| {
                                    let p = p.as_os_str().to_string_lossy();
                                    match typ {
                                        FType::Directory => (),
                                        FType::File => {
                                            let src_path = src_path.append(&p);
                                            dp.remove_advertisement(&src_path);
                                            self.aliased.remove(&src_path);
                                        }
                                    }
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
                                update.current.iter_children_no_pfx(target, |p, typ| {
                                    let p = p.as_os_str().to_string_lossy();
                                    match typ {
                                        FType::Directory => (),
                                        FType::File => {
                                            let src_path = src_path.append(&p);
                                            let tgt_path = tgt_path.append(&p);
                                            if let Err(e) = dp.advertise(src_path.clone()) {
                                                warn!("failed to advertise {}, {}", src_path, e)
                                            }
                                            self.aliased.insert(src_path, tgt_path);
                                        }
                                    }
                                })
                            }
                        },
                    },
                },
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
    let st = Instant::now();
    dbg!(st.elapsed());
    let publisher = Publisher::new(cfg, auth, opts.bind).await?;
    let (tx_file_updates, rx_file_updates) = mpsc::unbounded();
    let (tx_structure_updates, rx_structure_updates) = mpsc::unbounded();
    let (tx_events, mut rx_events) = mpsc::unbounded();
    publisher.events(tx_events);
    let mut rx_file_updates = Batched::new(rx_file_updates, 10_000);
    let sysfs = Arc::new(opts.sysfs);
    let paths = Paths::new(sysfs.clone(), opts.base.clone());
    let file_poller = FilePoller::new(tx_file_updates);
    let structure_poller = StructurePoller::new(tx_structure_updates);
    let mut dp = publisher.publish_default(opts.base.clone())?;
    let mut published = Published::new(opts.base.clone(), opts.sysfs.clone());
    let mut updates = publisher.start_batch();
    loop {
        select_biased! {
            (p, reply) = dp.select_next_some() => match published.resolve(&p) {
                None => if let Some(mut fspath) = paths.fs_path(&p) {
                    fspath.pop();
                    let fspath = if fspath.starts_with(&*sysfs) {
                        Arc::new(fspath)
                    } else {
                        sysfs.clone()
                    };
                    match structure_poller.start(fspath).await {
                        Ok(Some(up)) => published.advertise(&dp, up),
                        Ok(None) => (), // already polling this
                        Err(e) => warn!("failed to poll "),
                    }
                }
                Some((fspath, id)) => match id {
                    Some(id) => match publisher.alias(*id, p.clone()) {
                        Ok(()) => (),
                        Err(e) => warn!("failed to alias {}, {}", p, e),
                    },
                    None => match file_poller.start(fspath.clone()).await {
                        Err(e) => warn!("polling file {} failed {}", p, e),
                        Ok((fid, v)) => {
                            let flags = PublishFlags::DESTROY_ON_IDLE;
                            match publisher.publish_with_flags(flags, p.clone(), v) {
                                Err(e) => warn!("failed to publish {}, {}", p, e),
                                Ok(val) => {
                                    let vid = val.id();
                                    *id = Some(vid);
                                    published.published.insert(vid, (p.clone(), val));
                                    published.by_fid.insert(fid, vid);
                                    let _ = reply.send(());
                                }
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
                        Some((_, val)) => val.update_changed(&mut updates, v),
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
                    if let Some((p, _)) = published.published.remove(&id) {
                        if let Some((_, ref mut id)) = published.resolve(&p) {
                            *id = None;
                        }
                    }
                }
            }
        }
    }
}
