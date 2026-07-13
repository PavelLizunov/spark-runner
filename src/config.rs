//! CLI definition, pinned `codex.lock` loading, and small filesystem helpers.

use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
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
    pub platform: String,
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
    #[error("locked runtime path is not an absolute regular executable: {0}")]
    InvalidExecutable(String),
    #[error("locked runtime platform {locked:?} does not match this host {host:?}")]
    PlatformMismatch { locked: String, host: String },
    #[error("locked {kind} SHA-256 does not match: expected {expected}, got {actual}")]
    HashMismatch {
        kind: &'static str,
        expected: String,
        actual: String,
    },
    #[error("locked runtime version {expected:?} does not match executable output")]
    VersionMismatch { expected: String },
    #[error("failed to execute locked runtime for version verification: {0}")]
    VersionCheck(io::Error),
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

    /// Verify the actual runtime immediately before it is handed to the
    /// launcher. `codex.lock` is an assertion about bytes on disk, not merely
    /// metadata parsed at startup.
    pub fn verify_for_spawn(&self) -> Result<PathBuf, ConfigError> {
        self.validate()?;
        let path = Path::new(&self.binary_path);
        if !path.is_absolute() {
            return Err(ConfigError::InvalidExecutable(self.binary_path.clone()));
        }
        let canonical = std::fs::canonicalize(path)
            .map_err(|_| ConfigError::InvalidExecutable(self.binary_path.clone()))?;
        let metadata = std::fs::metadata(&canonical)
            .map_err(|_| ConfigError::InvalidExecutable(canonical.display().to_string()))?;
        if !metadata.is_file() || !is_executable(&metadata) {
            return Err(ConfigError::InvalidExecutable(
                canonical.display().to_string(),
            ));
        }
        let host = format!("{}-{}", env::consts::OS, env::consts::ARCH);
        if self.platform != host {
            return Err(ConfigError::PlatformMismatch {
                locked: self.platform.clone(),
                host,
            });
        }
        verify_hash("runtime", &canonical, &self.sha256)?;

        let schema = Path::new(&self.schema_path);
        let schema = std::fs::canonicalize(schema).map_err(|_| ConfigError::HashMismatch {
            kind: "schema",
            expected: self.schema_hash.clone(),
            actual: "unreadable".to_string(),
        })?;
        if !schema.is_file() {
            return Err(ConfigError::HashMismatch {
                kind: "schema",
                expected: self.schema_hash.clone(),
                actual: "not-a-regular-file".to_string(),
            });
        }
        verify_hash("schema", &schema, &self.schema_hash)?;

        let output = ProcessCommand::new(&canonical)
            .arg("--version")
            .output()
            .map_err(ConfigError::VersionCheck)?;
        let reported = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() || !reported.split_whitespace().any(|word| word == self.version)
        {
            return Err(ConfigError::VersionMismatch {
                expected: self.version.clone(),
            });
        }
        Ok(canonical)
    }
}

fn verify_hash(kind: &'static str, path: &Path, expected: &str) -> Result<(), ConfigError> {
    let actual = sha256_hex(&std::fs::read(path).map_err(|_| ConfigError::HashMismatch {
        kind,
        expected: expected.to_string(),
        actual: "unreadable".to_string(),
    })?);
    if actual == expected {
        Ok(())
    } else {
        Err(ConfigError::HashMismatch {
            kind,
            expected: expected.to_string(),
            actual,
        })
    }
}

#[cfg(unix)]
fn is_executable(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_metadata: &std::fs::Metadata) -> bool {
    true
}

// A tiny in-tree SHA-256 implementation keeps the runtime pin self-contained
// and avoids adding an unreviewed dependency solely for a pre-spawn check.
fn sha256_hex(input: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut data = input.to_vec();
    let bits = (data.len() as u64).wrapping_mul(8);
    data.push(0x80);
    while data.len() % 64 != 56 {
        data.push(0);
    }
    data.extend_from_slice(&bits.to_be_bytes());
    let mut h = [
        0x6a09e667_u32,
        0xbb67ae85,
        0x3c6ef372,
        0xa54ff53a,
        0x510e527f,
        0x9b05688c,
        0x1f83d9ab,
        0x5be0cd19,
    ];
    for block in data.chunks_exact(64) {
        let mut w = [0_u32; 64];
        for (index, word) in w[..16].iter_mut().enumerate() {
            *word = u32::from_be_bytes(block[index * 4..index * 4 + 4].try_into().expect("chunk"));
        }
        for index in 16..64 {
            let s0 = w[index - 15].rotate_right(7)
                ^ w[index - 15].rotate_right(18)
                ^ (w[index - 15] >> 3);
            let s1 = w[index - 2].rotate_right(17)
                ^ w[index - 2].rotate_right(19)
                ^ (w[index - 2] >> 10);
            w[index] = w[index - 16]
                .wrapping_add(s0)
                .wrapping_add(w[index - 7])
                .wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choice = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(choice)
                .wrapping_add(K[index])
                .wrapping_add(w[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(majority);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    h.iter().map(|word| format!("{word:08x}")).collect()
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

#[cfg(test)]
mod tests {
    use super::sha256_hex;

    #[test]
    fn sha256_matches_the_standard_test_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
