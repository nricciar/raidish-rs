//! Running the host's own tools (virsh, mkfs, blkid, the attach helpers):
//! their output as text, and failures as what the tool said.

use tokio::process::Command;

/// How a command finished, its streams as text.
pub struct Ran {
    /// Its exit code (None: killed by a signal).
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl Ran {
    pub fn ok(&self) -> bool {
        self.code == Some(0)
    }
}

/// The program `cmd` runs, for messages.
fn program(cmd: &Command) -> String {
    cmd.as_std().get_program().to_string_lossy().into_owned()
}

/// Run `cmd` to the end (killed if the caller gives up on it). Err: it
/// couldn't be started.
pub async fn ran(mut cmd: Command) -> Result<Ran, String> {
    let out = cmd
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|e| format!("{}: {}", program(&cmd), e))?;
    Ok(Ran {
        code: out.status.code(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    })
}

/// Run `cmd`; its output (trimmed) if it succeeded, else the command and
/// what it said on stderr.
pub async fn run(cmd: Command) -> Result<String, String> {
    let what = {
        let c = cmd.as_std();
        std::iter::once(c.get_program())
            .chain(c.get_args())
            .map(|a| a.to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join(" ")
    };
    let r = ran(cmd).await?;
    if r.ok() {
        Ok(r.stdout.trim().to_string())
    } else {
        Err(format!("{}: {}", what, r.stderr.trim()))
    }
}

#[cfg(test)]
#[path = "tests/proc_tests.rs"]
mod tests;
