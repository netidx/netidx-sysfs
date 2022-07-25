#[macro_use]
extern crate lazy_static;

mod sysfs;
use anyhow::Result;
use netidx::{
    path::Path,
    publisher::{BindCfg, Id, Publisher, Val},
};
use netidx_tools::ClientParams;
use std::path::PathBuf;
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

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Params::from_args();
    let (cfg, auth) = opts.common.load();
    let publisher = Publisher::new(cfg, auth, opts.bind).await?;
}
