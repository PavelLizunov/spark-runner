//! Child process lifecycle: spawn without a shell, drain stderr concurrently
//! into a bounded tail, and guarantee kill/wait on shutdown or drop (ADR-002).
//!
//! On Unix the launcher is spawned as its own process group leader so that
//! any native descendant it forks (e.g. an npm/Node launcher spawning the
//! real app-server) is terminated along with it, instead of being orphaned
//! while still holding the stderr pipe open.

use std::collections::VecDeque;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::config::VerifiedSubscriptionAuth;

pub const STDERR_TAIL_BYTES: usize = 16 * 1024;
/// Upper bound on kill/wait and stderr-task join during shutdown, so cleanup
/// can never hang even if a process-group kill somehow fails to land.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Process-group management for the Unix case described above. Uses a raw
/// `kill(2)` FFI declaration instead of the `libc` crate: the symbol is
/// already linked into every Unix binary via the system libc, so this needs
/// no extra dependency.
#[cfg(unix)]
mod unix {
    use tokio::process::Command;

    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }

    pub const SIGKILL: i32 = 9;

    /// Make the spawned child the leader of a brand-new process group
    /// (`pgid == pid`), so its whole descendant tree can be targeted as a
    /// unit later instead of just the launcher itself.
    pub fn isolate_process_group(command: &mut Command) {
        command.process_group(0);
    }

    /// Send `sig` to every process in group `pgid` (a negative pid targets
    /// the whole group). SAFETY: `kill` only reads its integer arguments; a
    /// stale or already-reaped pgid just yields ESRCH, which is ignored.
    pub fn kill_process_group(pgid: i32, sig: i32) {
        unsafe {
            kill(-pgid, sig);
        }
    }
}

#[derive(Debug, Default)]
struct StderrTail {
    bytes: VecDeque<u8>,
    total_seen: usize,
}

impl StderrTail {
    fn push(&mut self, chunk: &[u8]) {
        self.total_seen = self.total_seen.saturating_add(chunk.len());
        for byte in chunk {
            if self.bytes.len() == STDERR_TAIL_BYTES {
                self.bytes.pop_front();
            }
            self.bytes.push_back(*byte);
        }
    }

    fn snapshot(&self) -> String {
        // Child stderr is untrusted. Do not render it into logs, journals, or
        // public errors; retain only bounded diagnostic metadata.
        format!("stderr_bytes_seen={}", self.total_seen)
    }
}

fn configure_environment(
    command: &mut Command,
    cwd: Option<&Path>,
    subscription_auth: Option<&mut VerifiedSubscriptionAuth>,
) -> Result<(), ProcessError> {
    command.env_clear();
    for name in ["LANG", "LC_ALL", "TZ"] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    if let Some(dir) = cwd {
        let codex_home = dir.join("codex-home");
        std::fs::create_dir_all(&codex_home).map_err(ProcessError::CodexHome)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&codex_home, std::fs::Permissions::from_mode(0o700))
                .map_err(ProcessError::CodexHome)?;
        }
        if let Some(subscription_auth) = subscription_auth {
            subscription_auth
                .provision_into(&codex_home.join("auth.json"))
                .map_err(ProcessError::SubscriptionAuth)?;
        }
        command.env("CODEX_HOME", codex_home);
    } else if subscription_auth.is_some() {
        return Err(ProcessError::MissingCodexHome);
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("failed to spawn {program}: {source}")]
    Spawn {
        program: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to prepare private CODEX_HOME: {0}")]
    CodexHome(std::io::Error),
    #[error("failed to provision selected subscription auth file: {0}")]
    SubscriptionAuth(crate::config::ConfigError),
    #[error("selected subscription auth requires a private CODEX_HOME")]
    MissingCodexHome,
    #[error("child process did not expose a piped {0} handle")]
    MissingHandle(&'static str),
}

pub struct SpawnedChild {
    pub process: ChildProcess,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
}

pub struct ChildProcess {
    child: Option<Child>,
    #[cfg(unix)]
    pgid: Option<i32>,
    stderr_tail: Arc<Mutex<StderrTail>>,
    stderr_task: Option<JoinHandle<()>>,
}

impl ChildProcess {
    /// Spawn `program` with `args` directly (no shell). stdin/stdout are piped
    /// for the JSONL client; stderr is drained concurrently into a bounded tail.
    pub fn spawn(
        program: &str,
        args: &[String],
        cwd: Option<&Path>,
    ) -> Result<SpawnedChild, ProcessError> {
        Self::spawn_with_subscription_auth(program, args, cwd, None)
    }

    /// Spawn using only the caller-selected, already-validated subscription
    /// credential. The source path is never passed to the child and no
    /// ambient Codex configuration is inherited.
    pub fn spawn_with_subscription_auth(
        program: &str,
        args: &[String],
        cwd: Option<&Path>,
        subscription_auth: Option<&mut VerifiedSubscriptionAuth>,
    ) -> Result<SpawnedChild, ProcessError> {
        let mut command = Command::new(program);
        command.args(args);
        if let Some(dir) = cwd {
            command.current_dir(dir);
        }
        configure_environment(&mut command, cwd, subscription_auth)?;
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        #[cfg(unix)]
        unix::isolate_process_group(&mut command);

        let mut child = command.spawn().map_err(|source| ProcessError::Spawn {
            program: program.to_string(),
            source,
        })?;
        #[cfg(unix)]
        let pgid = child.id().map(|id| id as i32);

        let stdin = child
            .stdin
            .take()
            .ok_or(ProcessError::MissingHandle("stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or(ProcessError::MissingHandle("stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or(ProcessError::MissingHandle("stderr"))?;

        let stderr_tail = Arc::new(Mutex::new(StderrTail::default()));
        let tail_for_task = Arc::clone(&stderr_tail);
        let stderr_task = tokio::spawn(async move {
            let mut stderr = BufReader::new(stderr);
            let mut chunk = [0_u8; 1024];
            while let Ok(read) = stderr.read(&mut chunk).await {
                if read == 0 {
                    break;
                }
                tail_for_task.lock().await.push(&chunk[..read]);
            }
        });

        Ok(SpawnedChild {
            process: ChildProcess {
                child: Some(child),
                #[cfg(unix)]
                pgid,
                stderr_tail,
                stderr_task: Some(stderr_task),
            },
            stdin,
            stdout,
        })
    }

    /// Sanitized-by-construction snapshot of the last stderr lines (no stdout/protocol content).
    pub async fn stderr_tail(&self) -> String {
        self.stderr_tail.lock().await.snapshot()
    }

    /// Testable bounded-retention metric. The bytes themselves are never
    /// rendered into errors, journals, or logs.
    pub async fn stderr_tail_len(&self) -> usize {
        self.stderr_tail.lock().await.bytes.len()
    }

    /// Kill the whole process group (so native descendants die too), wait for
    /// the child to exit, then join the stderr drain task. Both waits are
    /// bounded by [`SHUTDOWN_TIMEOUT`] so a stuck kill can never hang shutdown.
    pub async fn shutdown(&mut self) {
        if let Some(mut child) = self.child.take() {
            #[cfg(unix)]
            let killed_group = if let Some(pgid) = self.pgid.take() {
                unix::kill_process_group(pgid, unix::SIGKILL);
                true
            } else {
                false
            };
            #[cfg(not(unix))]
            let killed_group = false;

            if !killed_group {
                let _ = child.start_kill();
            }

            if tokio::time::timeout(SHUTDOWN_TIMEOUT, child.wait())
                .await
                .is_err()
            {
                tracing::warn!("child process wait timed out during shutdown; abandoning wait");
            }
        }
        if let Some(task) = self.stderr_task.take() {
            let abort_handle = task.abort_handle();
            if tokio::time::timeout(SHUTDOWN_TIMEOUT, task).await.is_err() {
                abort_handle.abort();
            }
        }
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            #[cfg(unix)]
            {
                if let Some(pgid) = self.pgid.take() {
                    unix::kill_process_group(pgid, unix::SIGKILL);
                } else {
                    let _ = child.start_kill();
                }
            }
            #[cfg(not(unix))]
            {
                let _ = child.start_kill();
            }
        }
        if let Some(task) = self.stderr_task.take() {
            task.abort();
        }
    }
}
