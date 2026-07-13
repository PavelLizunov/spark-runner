mod client;
mod config;
mod jsonl;
mod process;
mod state;

use std::process::ExitCode;

use clap::Parser;
use serde_json::Value;

use client::{ClientError, CodexClient};
use config::{Cli, CodexLock, Command, ConfigError, DEFAULT_CODEX_LOCK};
use process::{ChildProcess, ProcessError};

/// Pinned live app-server binary; the exact path/version/sha256 also live in `codex.lock`.
const LIVE_ARGS: &[&str] = &["app-server", "--listen", "stdio://"];

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Process(#[from] ProcessError),
    #[error(transparent)]
    Client(#[from] ClientError),
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Doctor { live } => run_doctor(live).await,
        Command::Run { prompt, live } => run_turn(prompt, live).await,
    };
    match result {
        Ok(summary) => {
            println!("{summary}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            tracing::error!(error = %err, "spark-runner failed");
            eprintln!("spark-runner: error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn launch_spec(live: bool) -> Result<(String, Vec<String>), AppError> {
    if live {
        let lock = CodexLock::load(std::path::Path::new(DEFAULT_CODEX_LOCK))?;
        lock.validate()?;
        Ok((
            lock.binary_path,
            LIVE_ARGS.iter().map(|arg| arg.to_string()).collect(),
        ))
    } else {
        let path = config::fake_app_server_path()?;
        Ok((path.to_string_lossy().to_string(), Vec::new()))
    }
}

async fn spawn_client(live: bool) -> Result<(CodexClient, std::path::PathBuf), AppError> {
    let (program, args) = launch_spec(live)?;
    let cwd = config::ephemeral_cwd()?;
    let spawned = ChildProcess::spawn(&program, &args, None)?;
    let client = CodexClient::new(spawned.process, spawned.stdin, spawned.stdout);
    Ok((client, cwd))
}

fn model_list_has_required_model(model_list: &Value) -> bool {
    model_list
        .get("data")
        .or_else(|| model_list.get("models"))
        .and_then(Value::as_array)
        .map(|models| {
            models.iter().any(|model| {
                model.get("id").and_then(Value::as_str) == Some(client::REQUIRED_MODEL)
            })
        })
        .unwrap_or(false)
}

async fn run_doctor(live: bool) -> Result<String, AppError> {
    let (mut client, cwd) = spawn_client(live).await?;

    client.initialize().await?;
    client.account_read().await?;
    let model_list = client.model_list().await?;
    if !model_list_has_required_model(&model_list) {
        return Err(AppError::Client(ClientError::FallbackModel {
            observed: "missing-from-model-list".to_string(),
            required: client::REQUIRED_MODEL,
        }));
    }
    client.rate_limits_read().await?;

    let thread = client.thread_start(&cwd).await?;
    client
        .turn_start(&thread.thread_id, "spark-runner doctor readiness check")
        .await?;
    let turn = client.wait_turn_completed().await?;
    if client.is_poisoned() {
        return Err(AppError::Client(ClientError::SessionPoisoned));
    }
    tracing::debug!(stderr_tail = %client.stderr_tail().await, "app-server stderr tail (diagnostic only)");
    client.shutdown().await?;
    let _ = std::fs::remove_dir_all(&cwd);

    Ok(format!(
        "doctor: ok mode={} model={} turn_status={}",
        mode_label(live),
        thread.model,
        turn.status
    ))
}

async fn run_turn(prompt: String, live: bool) -> Result<String, AppError> {
    let (mut client, cwd) = spawn_client(live).await?;

    client.initialize().await?;
    let thread = client.thread_start(&cwd).await?;
    client.turn_start(&thread.thread_id, &prompt).await?;
    let turn = client.wait_turn_completed().await?;
    if client.is_poisoned() {
        return Err(AppError::Client(ClientError::SessionPoisoned));
    }
    tracing::debug!(stderr_tail = %client.stderr_tail().await, "app-server stderr tail (diagnostic only)");
    client.shutdown().await?;
    let _ = std::fs::remove_dir_all(&cwd);

    Ok(format!(
        "run: mode={} model={} turn_status={}",
        mode_label(live),
        thread.model,
        turn.status
    ))
}

fn mode_label(live: bool) -> &'static str {
    if live {
        "live"
    } else {
        "offline"
    }
}
