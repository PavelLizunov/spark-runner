//! Library surface for integration tests (and the `fake_app_server` fixture
//! binary, which reuses [`jsonl::MAX_FRAME_LEN`]). The `spark-runner` binary
//! also uses this crate directly.

pub mod client;
pub mod config;
pub mod journal;
pub mod jsonl;
pub mod orchestrator;
pub mod process;
pub mod state;
