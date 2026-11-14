//! Bounded graceful termination for participating workers.
use std::{
    error::Error,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Default)]
pub struct Stop {
    requested: Arc<AtomicBool>,
    wake: Arc<tokio::sync::Notify>,
}
impl Stop {
    pub fn requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
    pub async fn wait(&self, duration: Duration) {
        if !self.requested() {
            tokio::select! { _ = self.wake.notified() => {}, _ = tokio::time::sleep(duration) => {} }
        }
    }
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        self.wake.notify_one();
    }
}

pub async fn run_until_signal<F, Fut>(work: F) -> Result<()>
where
    F: FnOnce(Stop) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    run_until_signal_with_setting("SWALLOW_WORKER_SHUTDOWN_GRACE_SECONDS", work).await
}

pub async fn run_until_signal_with_setting<F, Fut>(setting: &str, work: F) -> Result<()>
where
    F: FnOnce(Stop) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let seconds = match std::env::var(setting) {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|n| (1..=300).contains(n))
            .ok_or("invalid shutdown grace")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid shutdown grace".into()),
    };
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let signal = async move {
        #[cfg(unix)]
        tokio::select! { _ = sigterm.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    };
    run_with_signal(work, signal, Duration::from_secs(seconds)).await
}

async fn run_with_signal<F, Fut, S>(work: F, signal: S, grace: Duration) -> Result<()>
where
    F: FnOnce(Stop) -> Fut,
    Fut: Future<Output = Result<()>>,
    S: Future<Output = ()>,
{
    let stop = Stop::default();
    let task = work(stop.clone());
    tokio::pin!(task);
    tokio::select! {
        result = &mut task => result,
        _ = signal => {
            stop.request();
            match tokio::time::timeout(grace, &mut task).await {
                Ok(result) => result,
                Err(_) => Err("graceful shutdown timed out".into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn signal_waits_for_active_work_and_sets_stop() {
        let (signal, received) = tokio::sync::oneshot::channel::<()>();
        let (entered, wait) = tokio::sync::oneshot::channel::<Stop>();
        let (release, done) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(run_with_signal(
            move |stop| async move {
                entered.send(stop.clone()).ok().unwrap();
                done.await.unwrap();
                assert!(stop.requested());
                Ok(())
            },
            async { received.await.unwrap() },
            Duration::from_secs(1),
        ));
        let stop = wait.await.unwrap();
        signal.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), stop.wait(Duration::from_secs(2)))
            .await
            .unwrap();
        assert!(stop.requested());
        assert!(!task.is_finished());
        release.send(()).unwrap();
        assert!(task.await.unwrap().is_ok());
    }
    #[tokio::test]
    async fn grace_expires_when_work_does_not_finish() {
        let result = run_with_signal(
            |_| async { std::future::pending::<Result<()>>().await },
            async {},
            Duration::from_millis(20),
        )
        .await;
        assert!(result.unwrap_err().to_string().contains("timed out"));
    }
}
