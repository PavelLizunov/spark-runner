//! CLI definition, pinned `codex.lock` loading, and small filesystem helpers.

use std::env;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
use serde::Deserialize;

pub const DEFAULT_CODEX_LOCK: &str = "codex.lock";
/// Live authentication is opt-in and never inferred from HOME, CODEX_HOME,
/// or any ambient Codex configuration.  Operators select this one file
/// explicitly; the launcher retains its verified handle through provisioning.
pub const SUBSCRIPTION_AUTH_FILE_ENV: &str = "SPARK_RUNNER_SUBSCRIPTION_AUTH_FILE";
const EXPECTED_CODEX_TRANSPORT: &str = "stdio";
const EXPECTED_CODEX_VERSION: &str = "0.144.3";
const EXPECTED_CODEX_SCHEMA_PATH: &str =
    "protocol/0.144.3/codex_app_server_protocol.v2.schemas.json";
const PLACEHOLDER_SCHEMA_HASH: &str = "generated-after-implementation";

/// A verified live executable kept open from byte verification through spawn.
/// On Linux the launcher executes `/proc/self/fd/N`, which names this exact
/// inode in the child rather than reopening the mutable pathname.
pub struct VerifiedExecutable {
    path: PathBuf,
    #[cfg(target_os = "linux")]
    file: std::fs::File,
}

/// An owner-only subscription-auth file that has been validated without
/// parsing its contents.  The file handle, rather than its pathname, is used
/// when provisioning a child home so a replacement race cannot change the
/// credential selected by the operator.
pub struct VerifiedSubscriptionAuth {
    file: std::fs::File,
}

impl VerifiedSubscriptionAuth {
    /// Copy the already-open credential bytes into the one private child
    /// `CODEX_HOME`.  This deliberately treats the file as opaque: no auth
    /// value is parsed, logged, returned, or retained by the runner.
    pub fn provision_into(&mut self, destination: &Path) -> Result<(), ConfigError> {
        self.file.rewind().map_err(ConfigError::SubscriptionAuth)?;
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)
            .map_err(ConfigError::SubscriptionAuth)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o600))
                .map_err(ConfigError::SubscriptionAuth)?;
        }
        if let Err(error) = io::copy(&mut self.file, &mut output) {
            let _ = std::fs::remove_file(destination);
            return Err(ConfigError::SubscriptionAuth(error));
        }
        output.flush().map_err(ConfigError::SubscriptionAuth)?;
        Ok(())
    }
}

impl VerifiedExecutable {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn program(&self) -> String {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            format!("/proc/self/fd/{}", self.file.as_raw_fd())
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.path.to_string_lossy().to_string()
        }
    }
}

#[cfg(target_os = "linux")]
fn make_fd_inheritable(file: &std::fs::File) -> Result<(), ConfigError> {
    use std::os::fd::AsRawFd;

    unsafe extern "C" {
        fn fcntl(fd: i32, cmd: i32, ...) -> i32;
    }
    const F_GETFD: i32 = 1;
    const F_SETFD: i32 = 2;
    const FD_CLOEXEC: i32 = 1;
    // SAFETY: fcntl reads only the supplied file descriptor and flags. The
    // descriptor is owned by `file` for this call and errors are propagated.
    let flags = unsafe { fcntl(file.as_raw_fd(), F_GETFD) };
    if flags < 0 {
        return Err(ConfigError::VerifiedHandle(io::Error::last_os_error()));
    }
    // SAFETY: same argument constraints as F_GETFD above.
    if unsafe { fcntl(file.as_raw_fd(), F_SETFD, flags & !FD_CLOEXEC) } < 0 {
        return Err(ConfigError::VerifiedHandle(io::Error::last_os_error()));
    }
    Ok(())
}

#[derive(Parser, Debug)]
#[command(
    name = "spark-runner",
    about = "Minimal Codex app-server runner",
    version
)]
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
    pub version: String,
    /// The app-server is the platform-native executable from the installed
    /// Codex package layout.  We execute this exact artifact directly: a
    /// mutable JavaScript/npm launcher is deliberately not a second trust
    /// root or part of the spawn path.
    pub native_path: String,
    pub native_sha256: String,
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
    #[error("failed to retain verified runtime handle: {0}")]
    VerifiedHandle(io::Error),
    #[error("{SUBSCRIPTION_AUTH_FILE_ENV} must select an absolute owner-only regular file")]
    InvalidSubscriptionAuthFile,
    #[error("failed to provision selected subscription auth file: {0}")]
    SubscriptionAuth(io::Error),
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
        if self.native_sha256.len() != 64 {
            return Err(ConfigError::InvalidSchemaHash(
                "invalid runtime hash".to_string(),
            ));
        }
        Ok(())
    }

    /// Verify the actual runtime immediately before it is handed to the
    /// launcher. `codex.lock` is an assertion about bytes on disk, not merely
    /// metadata parsed at startup.
    pub fn verify_for_spawn(&self) -> Result<PathBuf, ConfigError> {
        Ok(self.verified_for_spawn()?.path)
    }

    /// Verify the open executable inode and retain its handle until the
    /// caller spawns it. This closes the check-then-reopen replacement race
    /// even when the package manager keeps the pinned file owner-writable.
    pub fn verified_for_spawn(&self) -> Result<VerifiedExecutable, ConfigError> {
        self.validate()?;
        let host = format!("{}-{}", env::consts::OS, env::consts::ARCH);
        if self.platform != host {
            return Err(ConfigError::PlatformMismatch {
                locked: self.platform.clone(),
                host,
            });
        }
        let native = Path::new(&self.native_path);
        if !native.is_absolute() {
            return Err(ConfigError::InvalidExecutable(self.native_path.clone()));
        }
        // A lock pins one immutable artifact. Canonicalising a caller-supplied
        // symlink merely follows a mutable indirection, so reject it before
        // any hash/version execution.
        if std::fs::symlink_metadata(native)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(true)
        {
            return Err(ConfigError::InvalidExecutable(self.native_path.clone()));
        }
        let native = std::fs::canonicalize(native)
            .map_err(|_| ConfigError::InvalidExecutable(self.native_path.clone()))?;
        let mut file = std::fs::File::open(&native)
            .map_err(|_| ConfigError::InvalidExecutable(native.display().to_string()))?;
        let native_metadata = file
            .metadata()
            .map_err(|_| ConfigError::InvalidExecutable(native.display().to_string()))?;
        if !native_metadata.is_file() || !is_executable(&native_metadata) {
            return Err(ConfigError::InvalidExecutable(native.display().to_string()));
        }
        // The checked-in lock records the native executable resolved by the
        // installed Codex launcher's documented vendor layout.  Refuse an
        // unrelated executable even if its bytes happen to be pinned.
        if !native.components().any(|part| part.as_os_str() == "vendor") {
            return Err(ConfigError::InvalidExecutable(native.display().to_string()));
        }
        verify_hash_file("native runtime", &mut file, &self.native_sha256)?;
        #[cfg(target_os = "linux")]
        make_fd_inheritable(&file)?;

        let schema = Path::new(&self.schema_path);
        if std::fs::symlink_metadata(schema)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(true)
        {
            return Err(ConfigError::HashMismatch {
                kind: "schema",
                expected: self.schema_hash.clone(),
                actual: "symlink".to_string(),
            });
        }
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

        #[cfg(target_os = "linux")]
        let version_program = {
            use std::os::fd::AsRawFd;
            format!("/proc/self/fd/{}", file.as_raw_fd())
        };
        #[cfg(not(target_os = "linux"))]
        let version_program = native.to_string_lossy().to_string();
        let output = ProcessCommand::new(version_program)
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
        Ok(VerifiedExecutable { path: native, file })
    }
}

/// Select and open the sole live subscription-auth file.  Nothing falls back
/// to the host `CODEX_HOME` or home directory: an absent, symlinked,
/// non-regular, or group/world-readable source fails closed before spawning.
pub fn selected_subscription_auth() -> Result<VerifiedSubscriptionAuth, ConfigError> {
    let path = env::var_os(SUBSCRIPTION_AUTH_FILE_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or(ConfigError::InvalidSubscriptionAuthFile)?;
    let metadata =
        std::fs::symlink_metadata(&path).map_err(|_| ConfigError::InvalidSubscriptionAuthFile)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ConfigError::InvalidSubscriptionAuthFile);
    }
    let file = open_subscription_auth(&path)?;
    let metadata = file
        .metadata()
        .map_err(|_| ConfigError::InvalidSubscriptionAuthFile)?;
    if !metadata.is_file() || !owner_only(&metadata) {
        return Err(ConfigError::InvalidSubscriptionAuthFile);
    }
    Ok(VerifiedSubscriptionAuth { file })
}

#[cfg(target_os = "linux")]
fn open_subscription_auth(path: &Path) -> Result<std::fs::File, ConfigError> {
    use std::os::unix::fs::OpenOptionsExt;

    // O_NOFOLLOW prevents a replacement between the metadata check and open
    // from redirecting this explicit capability through a symlink.
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0o400000)
        .open(path)
        .map_err(ConfigError::SubscriptionAuth)
}

#[cfg(not(target_os = "linux"))]
fn open_subscription_auth(path: &Path) -> Result<std::fs::File, ConfigError> {
    std::fs::File::open(path).map_err(ConfigError::SubscriptionAuth)
}

#[cfg(unix)]
fn owner_only(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    metadata.permissions().mode() & 0o077 == 0 && metadata.uid() == unsafe { geteuid() }
}

#[cfg(unix)]
unsafe extern "C" {
    fn geteuid() -> u32;
}

#[cfg(not(unix))]
fn owner_only(_metadata: &std::fs::Metadata) -> bool {
    true
}

fn verify_hash_file(
    kind: &'static str,
    file: &mut std::fs::File,
    expected: &str,
) -> Result<(), ConfigError> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|_| ConfigError::HashMismatch {
            kind,
            expected: expected.to_string(),
            actual: "unreadable".to_string(),
        })?;
    file.rewind().map_err(ConfigError::VerifiedHandle)?;
    let actual = sha256_hex(&bytes);
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
    use std::fs;
    #[cfg(unix)]
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{sha256_hex, CodexLock, ConfigError};

    #[test]
    fn sha256_matches_the_standard_test_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[cfg(unix)]
    struct HermeticLockFixture {
        root: PathBuf,
        lock: CodexLock,
    }

    #[cfg(unix)]
    impl Drop for HermeticLockFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[cfg(unix)]
    fn hermetic_lock_fixture(reported_version: &str) -> HermeticLockFixture {
        use std::os::unix::fs::PermissionsExt;

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("spark-runner-lock-{unique}"));
        let vendor = root.join("vendor/test/bin");
        fs::create_dir_all(&vendor).expect("vendor");
        let native = vendor.join("codex");
        let bytes = format!("#!/bin/sh\nprintf '%s\\n' 'codex-cli {reported_version}'\n");
        fs::write(&native, &bytes).expect("native");
        let mut permissions = fs::metadata(&native).expect("metadata").permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&native, permissions).expect("chmod");

        let mut lock =
            CodexLock::load(std::path::Path::new(super::DEFAULT_CODEX_LOCK)).expect("checked lock");
        lock.native_path = native.display().to_string();
        lock.native_sha256 = sha256_hex(bytes.as_bytes());
        HermeticLockFixture { root, lock }
    }

    /// T09: the launcher is not treated as the executable.  A changed native
    /// payload is rejected before its version command can be invoked.
    #[cfg(unix)]
    #[test]
    fn native_runtime_bytes_are_pinned_before_spawn() {
        use std::os::unix::fs::PermissionsExt;

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("spark-runner-pin-{unique}"));
        let vendor = root.join("vendor/test/bin");
        fs::create_dir_all(&vendor).expect("vendor");
        let native = vendor.join("codex");
        fs::write(&native, b"#!/bin/sh\necho codex-cli 0.144.3\n").expect("native");
        let mut permissions = fs::metadata(&native).expect("metadata").permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&native, permissions).expect("chmod");
        let mut lock =
            CodexLock::load(std::path::Path::new(super::DEFAULT_CODEX_LOCK)).expect("checked lock");
        lock.native_path = native.display().to_string();
        assert!(matches!(
            lock.verify_for_spawn(),
            Err(ConfigError::HashMismatch {
                kind: "native runtime",
                ..
            })
        ));
        let _ = fs::remove_dir_all(root);
    }

    /// T09: a symlink is mutable indirection and is rejected before any
    /// version command can run, even if it currently points at a vendor path.
    #[cfg(unix)]
    #[test]
    fn symlinked_runtime_is_rejected_before_spawn() {
        use std::os::unix::fs::symlink;

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("spark-runner-symlink-{unique}"));
        let vendor = root.join("vendor/test/bin");
        fs::create_dir_all(&vendor).expect("vendor");
        let target = vendor.join("target");
        fs::write(&target, b"not executed").expect("target");
        let link = vendor.join("codex");
        symlink(&target, &link).expect("symlink");
        let mut lock =
            CodexLock::load(std::path::Path::new(super::DEFAULT_CODEX_LOCK)).expect("checked lock");
        lock.native_path = link.display().to_string();
        assert!(matches!(
            lock.verify_for_spawn(),
            Err(ConfigError::InvalidExecutable(_))
        ));
        let _ = fs::remove_dir_all(root);
    }

    /// T09 matching case: the lock accepts a pinned vendor binary and the
    /// checked-in generated schema before spawn.
    #[cfg(unix)]
    #[test]
    fn checked_native_runtime_and_generated_schema_match_before_spawn() {
        let fixture = hermetic_lock_fixture("0.144.3");
        let verified = fixture
            .lock
            .verify_for_spawn()
            .expect("matching native pin");
        assert_eq!(
            verified,
            std::fs::canonicalize(&fixture.lock.native_path).unwrap()
        );
    }

    /// T09: mutable lock inputs cannot downgrade platform, generated-schema,
    /// or runtime-version assertions before a live process is admitted.
    #[cfg(unix)]
    #[test]
    fn platform_schema_and_version_mismatches_fail_closed() {
        let fixture = hermetic_lock_fixture("0.144.3");

        let mut wrong_platform = fixture.lock.clone();
        wrong_platform.platform = "definitely-not-this-host".to_string();
        assert!(matches!(
            wrong_platform.verify_for_spawn(),
            Err(ConfigError::PlatformMismatch { .. })
        ));

        let mut wrong_schema = fixture.lock.clone();
        wrong_schema.schema_hash = "0".repeat(64);
        assert!(matches!(
            wrong_schema.verify_for_spawn(),
            Err(ConfigError::HashMismatch { kind: "schema", .. })
        ));

        let wrong_version = hermetic_lock_fixture("0.0.0-not-codex");
        assert!(matches!(
            wrong_version.lock.verify_for_spawn(),
            Err(ConfigError::VersionMismatch { .. })
        ));
    }

    /// T09: verification retains an executable inode, not a pathname. This
    /// closes the deterministic replacement race between hashing and launch.
    #[cfg(target_os = "linux")]
    #[test]
    fn verified_handle_executes_pinned_inode_after_path_replacement() {
        use std::os::unix::fs::PermissionsExt;

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("spark-runner-race-{unique}"));
        let vendor = root.join("vendor/test/bin");
        fs::create_dir_all(&vendor).expect("vendor");
        let native = vendor.join("codex");
        let original = b"#!/bin/sh\necho codex-cli 0.144.3\n";
        fs::write(&native, original).expect("original executable");
        let mut permissions = fs::metadata(&native).expect("metadata").permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&native, permissions).expect("chmod");

        let mut lock =
            CodexLock::load(std::path::Path::new(super::DEFAULT_CODEX_LOCK)).expect("checked lock");
        lock.native_path = native.display().to_string();
        lock.native_sha256 = sha256_hex(original);
        let verified = lock.verified_for_spawn().expect("verify original inode");

        let replacement = vendor.join("replacement");
        fs::write(&replacement, b"#!/bin/sh\necho replaced\n").expect("replacement");
        let mut replacement_permissions = fs::metadata(&replacement)
            .expect("replacement metadata")
            .permissions();
        replacement_permissions.set_mode(0o700);
        fs::set_permissions(&replacement, replacement_permissions).expect("replacement chmod");
        fs::rename(&replacement, &native).expect("atomic replacement");

        let output = std::process::Command::new(verified.program())
            .output()
            .expect("execute retained inode");
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).trim(),
            "codex-cli 0.144.3"
        );
        let _ = fs::remove_dir_all(root);
    }
}
