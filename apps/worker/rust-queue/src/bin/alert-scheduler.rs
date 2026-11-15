//! Optional fixed-delay scheduler for the existing multi-destination alert command.
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::{error::Error, process::Stdio, time::Duration};
use tokio::process::{Child, Command};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

// A forced scheduler shutdown must not leave the fanout tree sending after its
// supervisor has reported termination. The group is private to this run.
struct FanoutChild {
    child: Child,
    #[cfg(unix)]
    group: Option<i32>,
    finished: bool,
}
impl FanoutChild {
    async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let status = self.child.wait().await;
        if status.is_ok() {
            self.finished = true;
        }
        status
    }
}
impl Drop for FanoutChild {
    fn drop(&mut self) {
        if !self.finished {
            #[cfg(unix)]
            if let Some(group) = self.group {
                // A negative PID addresses the private process group. The group
                // remains present while the child or its descendants are alive.
                unsafe {
                    libc::kill(-group, libc::SIGKILL);
                }
            }
            let _ = self.child.start_kill();
        }
    }
}

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
    let run_timeout = match std::env::var("SWALLOW_ALERT_SCHEDULER_RUN_TIMEOUT_SECONDS") {
        Err(std::env::VarError::NotPresent) => None,
        Ok(value) if value == "0" => None,
        Ok(value) => Some(Duration::from_secs(
            value
                .parse::<u64>()
                .ok()
                .filter(|n| (1..=86400).contains(n))
                .ok_or("invalid scheduler run timeout")?,
        )),
        Err(_) => return Err("invalid scheduler run timeout".into()),
    };
    let mut binary = std::env::current_exe()?;
    binary.set_file_name(format!("alert-fanout{}", std::env::consts::EXE_SUFFIX));
    while !stop.requested() {
        let mut command = Command::new(&binary);
        command
            .args(alert_args)
            .stdin(Stdio::null())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.as_std_mut().process_group(0);
        let status = match command.spawn() {
            Ok(child) => {
                #[cfg(unix)]
                let group = child.id().and_then(|id| i32::try_from(id).ok());
                let mut child = FanoutChild {
                    child,
                    #[cfg(unix)]
                    group,
                    finished: false,
                };
                match run_timeout {
                    Some(limit) => match tokio::time::timeout(limit, child.wait()).await {
                        Ok(status) => (Some(status), false),
                        Err(_) => {
                            // Dropping this guard kills the private process group. The
                            // provider may have accepted a request before termination.
                            drop(child);
                            (None, true)
                        }
                    },
                    None => (Some(child.wait().await), false),
                }
            }
            Err(error) => (Some(Err(error)), false),
        };
        match status {
            (None, true) => {
                eprintln!("alert scheduler: run timed out; delivery outcome unknown");
                if exit_on_failure {
                    return Err("alert scheduler run timed out".into());
                }
            }
            (Some(Ok(status)), false) if status.success() => println!("alert scheduler: completed"),
            (Some(Ok(status)), false) if status.code() == Some(2) => {
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
