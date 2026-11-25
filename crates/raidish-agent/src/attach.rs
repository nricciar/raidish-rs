//! Kernel devices for volumes: the root helper (`raidish-attach`) that
//! attaches and detaches them over NBD or NVMe/TCP.

use crate::AutomountConfig;
use crate::inode::Transport;
use crate::types::DatasetName;
use async_trait::async_trait;
use std::time::Duration;

/// The longest one helper call may take. Its own waits (the connect, then
/// the device showing up, `udevadm settle`) come to about 20 s. Past this
/// it counts as failed (an attach is tried again `ATTACH_RETRY` later),
/// so a hung `nvme connect` or `nbd-client` can't hold up the agent loop.
pub(crate) const HELPER_TIMEOUT: Duration = Duration::from_secs(60);

/// Attaches and detaches the kernel devices of volumes exported here.
#[async_trait]
pub(crate) trait Attacher: Send + Sync {
    /// Attach volume `name`, exported here over `t`: its device, or None
    /// when there's no helper (exported only).
    async fn attach(&self, name: &DatasetName, t: Transport) -> Result<Option<String>, String>;

    async fn detach(&self, name: &DatasetName, t: Transport) -> Result<(), String>;
}

/// The root helper, as the `automount` config names it.
pub(crate) struct Helper {
    /// None: `helper: none`, export only.
    path: Option<String>,
    /// Running as root: the helper runs directly, else through `sudo -n`.
    root: bool,
    bind: String,
    nbd_port: u16,
    nvme_port: u16,
    timeout: Duration,
}

impl Helper {
    pub fn new(config: &AutomountConfig) -> Self {
        Helper {
            path: config.helper(),
            root: is_root(),
            bind: config.bind(),
            nbd_port: config.nbd_port(),
            nvme_port: config.nvme_port(),
            timeout: HELPER_TIMEOUT,
        }
    }

    /// Run the helper with `args`: the last line it printed, if any.
    async fn run(&self, args: &[&str]) -> Result<Option<String>, String> {
        let Some(helper) = &self.path else {
            return Ok(None);
        };
        if !tokio::fs::try_exists(helper).await.unwrap_or(false) {
            return Err(format!("{} not installed", helper));
        }
        let mut cmd = if self.root {
            tokio::process::Command::new(helper)
        } else {
            let mut c = tokio::process::Command::new("sudo");
            c.arg("-n").arg(helper);
            c
        };
        cmd.args(args);
        // Given up on, the child is killed (`proc::ran`). Through sudo that
        // kills sudo, not the helper under it: the loop goes on either way.
        let out = tokio::time::timeout(self.timeout, crate::proc::ran(cmd))
            .await
            .map_err(|_| format!("{} {} timed out after {:?}", helper, args[0], self.timeout))??;
        if !out.ok() {
            return Err(out.stderr.trim().to_string());
        }
        Ok(out
            .stdout
            .lines()
            .rev()
            .map(str::trim)
            .find(|l| !l.is_empty())
            .map(str::to_string))
    }
}

#[async_trait]
impl Attacher for Helper {
    async fn attach(&self, name: &DatasetName, t: Transport) -> Result<Option<String>, String> {
        let (cmd, port) = match t {
            Transport::Nbd => ("nbd-attach", self.nbd_port),
            Transport::Nvme => ("nvme-attach", self.nvme_port),
        };
        let port = port.to_string();
        self.run(&[cmd, name, &self.bind, &port]).await
    }

    async fn detach(&self, name: &DatasetName, t: Transport) -> Result<(), String> {
        let cmd = match t {
            Transport::Nbd => "nbd-detach",
            Transport::Nvme => "nvme-detach",
        };
        self.run(&[cmd, name]).await.map(|_| ())
    }
}

/// Whether this process runs as root (its real uid, from
/// `/proc/self/status`; read once, at startup).
fn is_root() -> bool {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("Uid:"))
                .map(|l| l.split_whitespace().nth(1) == Some("0"))
        })
        .unwrap_or(false)
}
