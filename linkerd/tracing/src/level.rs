use linkerd_error::Error;
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::trace;
use tracing_subscriber::{
    filter::{self, EnvFilter, LevelFilter},
    reload, Layer, Registry,
};

/// Gets and sets the log level of a tracing subscriber.
#[derive(Clone)]
pub struct Handle {
    inner: reload::Handle<FilteredLayer, Registry>,
    // Used only by startup/admin configuration, never by tracing callbacks.
    workers: Arc<Mutex<Vec<reload::Handle<FilteredLayer, Registry>>>>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn workers_are_independent_and_admin_reload_reaches_all_registered_workers() {
        let (_global, parent) =
            crate::Settings::for_test("warn".into(), "plain".into()).build_scoped();
        let (_a, a) = crate::Settings::for_test("warn".into(), "plain".into()).build_scoped();
        let (_b, b) = crate::Settings::for_test("warn".into(), "plain".into()).build_scoped();
        let (parent, a, b) = (
            parent.level().unwrap(),
            a.level().unwrap(),
            b.level().unwrap(),
        );
        parent.add_worker(a).unwrap();
        parent.add_worker(b).unwrap();
        a.set_level("debug").unwrap();
        assert_eq!(a.current().unwrap(), "_access_log=off,debug");
        assert_eq!(b.current().unwrap(), "_access_log=off,warn");
        parent.set_level("error").unwrap();
        for handle in [parent, a, b] {
            assert_eq!(handle.current().unwrap(), "_access_log=off,error");
        }
        assert!(parent.set_level("[bad").is_err());
        assert_eq!(a.current().unwrap(), "_access_log=off,error");
    }

    #[test]
    fn scoped_off_preserves_disabled_reload_handle() {
        let (_dispatch, handle) =
            crate::Settings::for_test("off".into(), "plain".into()).build_scoped();
        assert!(handle.level().is_none());
    }
}

/// Returns an `EnvFilter` builder with the configuration used for parsing new
/// filter strings.
pub(crate) fn filter_builder() -> filter::Builder {
    EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into())
        // Disable regular expression matching for `fmt::Debug` fields, use
        // exact string matching instead.
        .with_regex(false)
}

type BoxLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;
pub(crate) type FilteredLayer = filter::Filtered<BoxLayer, EnvFilter, Registry>;

impl Handle {
    pub(crate) fn new(handle: reload::Handle<FilteredLayer, Registry>) -> Self {
        Self {
            inner: handle,
            workers: Arc::default(),
        }
    }

    /// Register a fresh worker filter before the admin server is started.
    /// Reload handles are weak: registering one does not retain its subscriber.
    pub fn add_worker(&self, worker: &Self) -> Result<(), Error> {
        worker.set_level(self.current()?)?;
        self.workers.lock().push(worker.inner.clone());
        Ok(())
    }

    pub fn set_from(&self, bytes: impl AsRef<[u8]>) -> Result<(), String> {
        let body = std::str::from_utf8(bytes.as_ref()).map_err(|e| format!("{e}"))?;
        trace!(request.body = ?body);
        self.set_level(body).map_err(|e| format!("{e}"))
    }

    pub fn set_level(&self, level: impl AsRef<str>) -> Result<(), Error> {
        let level = level.as_ref();
        let filter = filter_builder().parse(level)?.add_directive(
            format!("{}=off", super::access_log::TRACE_TARGET)
                .parse()
                .unwrap(),
        );
        self.inner.modify(|layer| {
            *layer.filter_mut() = filter.clone();
        })?;
        for worker in self.workers.lock().iter() {
            worker.modify(|layer| *layer.filter_mut() = filter.clone())?;
        }
        tracing::info!(%level, "set new log level");
        Ok(())
    }

    pub fn current(&self) -> Result<String, Error> {
        self.inner
            .with_current(|f| format!("{}", f.filter()))
            .map_err(Into::into)
    }
}
