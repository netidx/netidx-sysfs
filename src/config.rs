use std::path::Path;
use anyhow::Result;
use globset::{Glob, GlobMatcher};

#[derive(Debug, Default, Clone, Copy, Deserialize)]
pub(crate) enum PollType {
    Periodic { structure: Option<u64>, contents: u64 },
    #[default]
    INotify
}

mod file {
    #[derive(Debug, Deserialize)]
    struct PubSpec {
        pattern: String,
        #[serde(default)]
        exclude: Option<String>,
        #[serde(default)]
        poll_type: super::PollType,
        #[serde(default)]
        max_file_size: u64,
    }
}

#[derive(Debug)]
pub(crate) struct PubSpec {
    pub(crate) pattern: GlobMatcher,
    pub(crate) exclude: Option<GlobMatcher>,
    pub(crate) poll_type: PollType,
    pub(crate) max_file_size: u64
}

pub(crate) fn load<P: AsRef<Path>>(path: P) -> Result<Vec<PubSpec>> {
    
}
