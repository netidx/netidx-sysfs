#[macro_use]
extern crate lazy_static;

mod sysfs;
use crate::sysfs::{FType, Fid, Files, Poller};
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
use std::{collections::HashMap, mem, ops::Bound, path::PathBuf, time::Duration};
use structopt::StructOpt;
use tokio;

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
    #[structopt(long = "sysfs", help = "the path to sysfs", default_value = "/sys")]
    sysfs: PathBuf,
}

fn netidx_path<P: AsRef<std::path::Path>>(base: &Path, path: P) -> Path {
    base.append(&path.as_ref().as_os_str().to_string_lossy())
}

struct Published {
    published: FxHashMap<Id, (Path, Val)>,
    by_fid: FxHashMap<Fid, Id>,
    advertised: HashMap<Path, (PathBuf, Option<Id>)>,
    aliased: HashMap<Path, Path>,
}

impl Published {
    fn resolve(&mut self, path: &Path) -> Option<&mut (PathBuf, Option<Id>)> {
        let tgt = self.aliased.get(path).cloned();
        match tgt {
            Some(tgt) => self.resolve(&tgt),
            None => self.advertised.get_mut(path),
        }
    }

    fn advertise(base: &Path, files: &Files, dp: &DefaultHandle) -> Self {
        let mut t = Published {
            published: HashMap::default(),
            by_fid: HashMap::default(),
            advertised: HashMap::default(),
            aliased: HashMap::default(),
        };
        for (path, typ) in &files.paths {
            match typ {
                FType::Directory => (),
                FType::File => {
                    let npath = netidx_path(base, path);
                    if let Err(e) = dp.advertise(npath.clone()) {
                        warn!("failed to advertise path {}, {}", npath, e)
                    }
                    t.advertised.insert(npath, (path.clone(), None));
                }
            }
        }
        for (src, tgt) in &files.symlinks {
            match files.resolve(tgt) {
                Err(e) => {
                    warn!("bad symlink {:?} -> {:?} {}", src, tgt, e)
                }
                Ok((tgt, typ)) => match typ {
                    FType::File => {
                        let src_path = netidx_path(base, src);
                        let tgt_path = netidx_path(base, tgt);
                        if t.advertised.contains_key(&tgt_path) {
                            if let Err(e) = dp.advertise(src_path.clone()) {
                                warn!("failed to advertise path {}, {}", src_path, e)
                            }
                            t.aliased.insert(src_path, tgt_path);
                        } else {
                            warn!("symlink to missing path {} -> {}", src_path, tgt_path)
                        }
                    }
                    FType::Directory => {
                        let src_path = netidx_path(base, src);
                        let tgt_path = netidx_path(base, tgt);
                        let children = files
                            .paths
                            .range::<std::path::Path, (Bound<&std::path::Path>, Bound<&std::path::Path>)>((
                                Bound::Excluded(tgt),
                                Bound::Unbounded,
                            ))
                            .take_while(|(p, _)| p.starts_with(tgt))
                            .map(|(p, t)| (p.strip_prefix(tgt), t));
                        for (p, typ) in children {
                            let p = match p {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            match typ {
                                FType::Directory => (),
                                FType::File => {
                                    let src_path = netidx_path(&src_path, p);
                                    let tgt_path = netidx_path(&tgt_path, p);
                                    if t.advertised.contains_key(&tgt_path) {
                                        if let Err(e) = dp.advertise(src_path.clone()) {
                                            warn!("failed to advertise path {}, {}", src_path, e)
                                        }
                                        t.aliased.insert(src_path, tgt_path);
                                    } else {
                                        warn!(
                                            "symlink to missing path {} -> {}",
                                            src_path, tgt_path
                                        )
                                    }
                                }
                            }
                        }
                    }
                },
            }
        }
        t
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opts = Params::from_args();
    let (cfg, auth) = opts.common.load();
    let timeout = opts.timeout.map(Duration::from_secs);
    let files = sysfs::Files::new(opts.sysfs)?;
    let publisher = Publisher::new(cfg, auth, opts.bind).await?;
    let (tx_updates, rx_updates) = mpsc::unbounded();
    let (tx_events, mut rx_events) = mpsc::unbounded();
    publisher.events(tx_events);
    let mut rx_updates = Batched::new(rx_updates, 10_000);
    let poller = Poller::new(tx_updates);
    let mut dp = publisher.publish_default(opts.base.clone())?;
    let mut published = Published::advertise(&opts.base, &files, &dp);
    let mut updates = publisher.start_batch();
    loop {
        select_biased! {
            (p, reply) = dp.select_next_some() => if let Some((fspath, ref mut id)) = published.resolve(&p) {
                match id {
                    Some(id) => match publisher.alias(*id, p.clone()) {
                        Ok(()) => (),
                        Err(e) => warn!("failed to alias {}, {}", p, e),
                    },
                    None => match poller.start(fspath.clone()).await {
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
            r = rx_updates.select_next_some() => match r {
                BatchItem::InBatch((fid, v)) => match published.by_fid.get(&fid) {
                    None => poller.stop(fid)?,
                    Some(id) => match published.published.get(id) {
                        None => poller.stop(fid)?,
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
