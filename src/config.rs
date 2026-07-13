//! CLI definition, pinned `codex.lock` loading, and small filesystem helpers.

use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
use serde::Deserialize;

pub const DEFAULT_CODEX_LOCK: &str = "codex.lock";
const EXPECTED_CODEX_VERSION: &str = "0.142.0";
const EXPECTED_CODEX_SHA256: &str =
    "d3be844c45c4fd89392536e56e1010963f94785592596b50cd0c45bb8a341406";
const EXPECTED_CODEX_TRANSPORT: &str = "stdio";
const EXPECTED_CODEX_SCHEMA_PATH: &str = "protocol/0.142.0/stable.schema.json";
const PLACEHOLDER_SCHEMA_HASH: &str = "generated-after-implementation";

#[derive(Parser, Debug)]
#[command(name = "spark-runner", about = "Minimal Codex app-server runner")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Verify app-server connectivity, account/model/rate-limit reads, and one ephemeral turn.
    Doctor {
        /// Use the pinned live `codex app-server` instead of the offline fake app-server.
        #[arg(long)]
        live: bool,
    },
    /// Run a single ephemeral turn with the given prompt.
    Run {
        #[arg(long)]
        prompt: String,
        /// Use the pinned live `codex app-server` instead of the offline fake app-server.
        #[arg(long)]
        live: bool,
    },
    /// Serve the CP6 local loopback HTTP/SSE API.
    Serve {
        /// Use the pinned live `codex app-server` instead of the offline fake app-server.
        #[arg(long)]
        live: bool,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct CodexLock {
    pub binary_path: String,
    pub version: String,
    pub sha256: String,
    pub transport: String,
    pub required_model: String,
    pub schema_path: String,
    pub schema_hash: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read codex.lock at {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to parse codex.lock: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("could not locate fake_app_server binary next to the current executable: {0}")]
    FakeServerNotFound(io::Error),
    #[error("could not create ephemeral working directory: {0}")]
    EphemeralDir(io::Error),
    #[error("codex.lock field {field} mismatch: expected {expected:?}, got {actual:?}")]
    LockMismatch {
        field: &'static str,
        expected: &'static str,
        actual: String,
    },
    #[error("codex.lock field schema_hash must be a non-empty real hash, got {0:?}")]
    InvalidSchemaHash(String),
}

impl CodexLock {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        Ok(serde_json::from_str(&raw)?)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_lock_field("version", EXPECTED_CODEX_VERSION, &self.version)?;
        validate_lock_field("sha256", EXPECTED_CODEX_SHA256, &self.sha256)?;
        validate_lock_field("transport", EXPECTED_CODEX_TRANSPORT, &self.transport)?;
        validate_lock_field(
            "required_model",
            crate::client::REQUIRED_MODEL,
            &self.required_model,
        )?;
        validate_lock_field("schema_path", EXPECTED_CODEX_SCHEMA_PATH, &self.schema_path)?;
        if self.schema_hash.is_empty() || self.schema_hash == PLACEHOLDER_SCHEMA_HASH {
            return Err(ConfigError::InvalidSchemaHash(self.schema_hash.clone()));
        }
        Ok(())
    }
}

fn validate_lock_field(
    field: &'static str,
    expected: &'static str,
    actual: &str,
) -> Result<(), ConfigError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ConfigError::LockMismatch {
            field,
            expected,
            actual: actual.to_string(),
        })
    }
}

/// The offline fake app-server is built as a sibling binary of `spark-runner`
/// in the same target directory, but integration tests may run from `target/debug/deps`.
pub fn fake_app_server_path() -> Result<PathBuf, ConfigError> {
    let current = env::current_exe().map_err(ConfigError::FakeServerNotFound)?;
    let dir = current.parent().ok_or_else(|| {
        ConfigError::FakeServerNotFound(io::Error::new(
            io::ErrorKind::NotFound,
            "no parent directory for current executable",
        ))
    })?;
    let candidates = [
        Some(dir.join("fake_app_server")),
        dir.parent().map(|p| p.join("fake_app_server")),
    ];
    candidates
        .into_iter()
        .flatten()
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| {
            ConfigError::FakeServerNotFound(io::Error::new(
                io::ErrorKind::NotFound,
                "fake_app_server binary not found",
            ))
        })
}

/// A fresh, empty, read-only-safe temp directory for an ephemeral thread's cwd.
pub fn ephemeral_cwd() -> Result<PathBuf, ConfigError> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let dir = env::temp_dir().join(format!("spark-runner-{}-{unique}", std::process::id()));
    std::fs::create_dir_all(&dir).map_err(ConfigError::EphemeralDir)?;
    Ok(dir)
}
