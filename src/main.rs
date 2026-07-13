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
                .map(|addr| format!("serve: listening on {addr}"))
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
