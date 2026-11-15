//! Optional fixed-delay scheduler for the existing multi-destination alert command.
use std::{error::Error, process::Stdio, time::Duration};
use tokio::process::Command;
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::main]
async fn main() -> Result<()> {
    novu_postgres_queue::shutdown::run_until_signal_with_setting(
        "SWALLOW_ALERT_SCHEDULER_SHUTDOWN_GRACE_SECONDS",
        run,
    )
    .await
}
async fn run(stop: novu_postgres_queue::shutdown::Stop) -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (interval, alert_args) = args.split_first().ok_or("usage: alert-scheduler INTERVAL_SECONDS delivery ENV GRACE HEARTBEAT COOLDOWN CONFIG | alert-scheduler INTERVAL_SECONDS database ENV LABEL COOLDOWN CONFIG")?;
    let seconds = interval
        .parse::<u64>()
        .ok()
        .filter(|n| (1..=86400).contains(n))
        .ok_or("invalid scheduler interval")?;
    match alert_args {
        [mode, _, _, _, _, _] if mode == "delivery" => {}
        [mode, _, _, _, _] if mode == "database" => {}
        _ => return Err("invalid scheduler arguments".into()),
    }
    let exit_on_failure = match std::env::var("SWALLOW_ALERT_SCHEDULER_FAILURE_MODE") {
        Err(std::env::VarError::NotPresent) => false,
        Ok(mode) if mode == "continue" => false,
        Ok(mode) if mode == "exit" => true,
        _ => return Err("invalid scheduler failure mode".into()),
    };
    let mut binary = std::env::current_exe()?;
    binary.set_file_name(format!("alert-fanout{}", std::env::consts::EXE_SUFFIX));
    while !stop.requested() {
        let status = Command::new(&binary)
            .args(alert_args)
            .stdin(Stdio::null())
            .status()
            .await;
        match status {
            Ok(status) if status.success() => println!("alert scheduler: completed"),
            Ok(status) if status.code() == Some(2) => {
                eprintln!("alert scheduler: attention required")
            }
            _ => {
                eprintln!("alert scheduler: run failed; monitor and investigate");
                if exit_on_failure {
                    return Err("alert scheduler run failed".into());
                }
            }
        }
        if !stop.requested() {
            stop.wait(Duration::from_secs(seconds)).await;
        }
    }
    Ok(())
}
