//! Per-connection drain, independent of the process-wide drain. An acceptor
//! runs a connection's future inside [`scope`]; the HTTP server picks the
//! signal up when it starts serving and shuts that one connection down
//! gracefully (HTTP/2 GOAWAY, then in-flight streams finish) once it fires.

use std::future::Future;
use tokio::sync::watch;

tokio::task_local! {
    static SIGNAL: watch::Receiver<bool>;
}

/// Runs `fut` with `signal` as the drain signal of the connection it serves.
pub fn scope<F: Future>(signal: watch::Receiver<bool>, fut: F) -> impl Future<Output = F::Output> {
    SIGNAL.scope(signal, fut)
}

/// The current connection's drain signal, if its acceptor set one.
pub(crate) fn current() -> Option<watch::Receiver<bool>> {
    SIGNAL.try_with(Clone::clone).ok()
}

/// Resolves once the signal fires; never without one or after its sender is
/// gone.
pub(crate) async fn signaled(signal: Option<watch::Receiver<bool>>) {
    if let Some(mut rx) = signal {
        if rx.wait_for(|drain| *drain).await.is_ok() {
            return;
        }
    }
    std::future::pending().await
}
