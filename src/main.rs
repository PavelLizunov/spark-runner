use std::process::ExitCode;

use clap::Parser;

use spark_runner::api::{serve, ApiConfig};
use spark_runner::config::{Cli, Command};
use spark_runner::orchestrator::{run_doctor, run_turn};

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Doctor { live } => run_doctor(live).await,
        Command::Run { prompt, live } => run_turn(prompt, live).await,
        Command::Serve { live } => match ApiConfig::from_env(live) {
            Ok(config) => serve(config)
                .await
                .map(|addr| format!("serve: stopped on {addr}"))
                .map_err(Into::into),
            Err(err) => Err(err.into()),
        },
    };
    match result {
        Ok(summary) => {
            println!("{summary}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            // AppError can wrap child-controlled or local filesystem text.
            // The CLI is a trust boundary just like HTTP and must not render
            // arbitrary diagnostics verbatim.
            let class = error_class(&err);
            tracing::error!(class, "spark-runner failed");
            eprintln!("spark-runner: error: {class}");
            ExitCode::FAILURE
        }
    }
}

fn error_class(error: &spark_runner::orchestrator::AppError) -> &'static str {
    match error {
        spark_runner::orchestrator::AppError::Config(_) => "configuration_failure",
        spark_runner::orchestrator::AppError::Process(_) => "process_failure",
        spark_runner::orchestrator::AppError::Client(error) => error.class(),
        spark_runner::orchestrator::AppError::Journal(_) => "journal_failure",
        spark_runner::orchestrator::AppError::Api(_) => "api_failure",
        spark_runner::orchestrator::AppError::EphemeralCleanup(_) => "cleanup_failure",
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
