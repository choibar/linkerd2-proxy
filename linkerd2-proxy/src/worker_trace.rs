//! Subscriber ownership for sharded workers. Runtime isolation alone does not
//! isolate EnvFilter/reload locks; each worker must build a new subscriber.
use linkerd_app::trace::{Handle, Settings};
use tracing::{dispatcher::DefaultGuard, Dispatch};

pub(crate) use linkerd_app::metrics::WorkerScope as Scope;

pub(crate) struct WorkerTrace {
    dispatch: Option<Dispatch>,
}

impl WorkerTrace {
    pub(crate) fn new(scope: Scope, parent: &Handle) -> Result<Self, String> {
        if scope == Scope::Shared {
            return Ok(Self { dispatch: None });
        }
        let (dispatch, handle) = Settings::from_env().build_scoped();
        if let (Some(parent), Some(worker)) = (parent.level(), handle.level()) {
            parent
                .add_worker(worker)
                .map_err(|e| format!("worker trace filter: {e}"))?;
        }
        Ok(Self {
            dispatch: Some(dispatch),
        })
    }

    pub(crate) fn enter(&self) -> Option<DefaultGuard> {
        self.dispatch.as_ref().map(tracing::dispatcher::set_default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Instrument;

    fn registry_id(dispatch: &Dispatch) -> usize {
        dispatch
            .downcast_ref::<linkerd_app::trace::registry::Registry>()
            .unwrap() as *const _ as usize
    }

    #[test]
    fn rejects_invalid_scope() {
        assert_eq!(Scope::parse("private").unwrap(), Scope::Private);
        assert_eq!(Scope::parse("shared").unwrap(), Scope::Shared);
        assert!(Scope::parse("worker").is_err());
    }

    #[test]
    fn span_created_before_spawn_and_spawned_tasks_use_the_workers_subscriber() {
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let (dispatch, _) = Settings::default().build_scoped();
            let trace = WorkerTrace {
                dispatch: Some(dispatch),
            };
            let expected = registry_id(trace.dispatch.as_ref().unwrap());
            // Match main.rs: construct the serve span while scoped on the
            // parent thread, then move the future to its worker OS thread.
            let span = {
                let _guard = trace.enter();
                tracing::error_span!("worker")
            };
            span.with_subscriber(|(_, d)| assert_eq!(registry_id(d), expected))
                .unwrap();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                let _guard = trace.enter();
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                barrier.wait();
                rt.block_on(async move {
                    tokio::spawn(
                        async move {
                            for _ in 0..16 {
                                assert_eq!(tracing::dispatcher::get_default(registry_id), expected);
                                tokio::task::yield_now().await;
                            }
                        }
                        .instrument(span),
                    )
                    .await
                    .unwrap();
                });
                expected
            }));
        }
        let ids: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert_ne!(ids[0], ids[1]);
    }
}
