mod report;
mod service;

pub use self::service::{NewHttpMetrics, ResponseBody};
use super::Report;
use linkerd_http_classify::ClassifyResponse;
use linkerd_metrics::{
    latency,
    legacy::{Counter, FmtMetrics, Histogram, LastUpdate, NewMetrics},
};
use linkerd_stack::{self as svc, layer};
use std::{collections::HashMap, fmt::Debug, hash::Hash};
use tokio::time::{Duration, Instant};

type Registry<T, C> = super::Registry<T, Metrics<C>>;

#[derive(Debug)]
pub struct Requests<T, C>(Registry<T, C>)
where
    T: Hash + Eq,
    C: Hash + Eq;

#[derive(Debug)]
pub struct Metrics<C>
where
    C: Hash + Eq,
{
    last_update: Instant,
    total: Counter,
    by_status: HashMap<Option<http::StatusCode>, StatusMetrics<C>>,
}

#[derive(Debug)]
pub struct StatusMetrics<C>
where
    C: Hash + Eq,
{
    latency: Histogram<latency::Ms>,
    by_class: HashMap<C, ClassMetrics>,
}

#[derive(Debug, Default)]
pub struct ClassMetrics {
    total: Counter,
}

// === impl Requests ===

impl<T: Hash + Eq, C: Hash + Eq> Default for Requests<T, C> {
    fn default() -> Self {
        Requests(Registry::default())
    }
}

impl<T: Hash + Eq, C: Hash + Eq> Requests<T, C> {
    pub fn into_report(self, retain_idle: Duration) -> Report<T, Metrics<C>>
    where
        Report<T, Metrics<C>>: FmtMetrics,
    {
        Report::new(retain_idle, self.0)
    }

    pub fn to_layer<L, N, Tgt>(
        &self,
    ) -> impl layer::Layer<N, Service = NewHttpMetrics<N, T, (), L, C, N::Service>> + Clone
    where
        L: ClassifyResponse<Class = C> + Send + Sync + 'static,
        N: svc::NewService<Tgt>,
    {
        self.to_layer_via(())
    }

    pub fn to_layer_via<L, N, Tgt, X>(
        &self,
        params: X,
    ) -> impl layer::Layer<N, Service = NewHttpMetrics<N, T, X, L, C, N::Service>> + Clone
    where
        L: ClassifyResponse<Class = C> + Send + Sync + 'static,
        N: svc::NewService<Tgt>,
        X: Clone,
    {
        let reg = self.0.clone();
        NewMetrics::layer_via(reg, params)
    }
}

impl<T: Hash + Eq, C: Hash + Eq> Clone for Requests<T, C> {
    fn clone(&self) -> Self {
        Requests(self.0.clone())
    }
}

// === impl Metrics ===

impl<C: Hash + Eq> Default for Metrics<C> {
    fn default() -> Self {
        Self {
            last_update: Instant::now(),
            total: Counter::default(),
            by_status: HashMap::default(),
        }
    }
}

#[cfg(feature = "test-util")]
impl<C: Hash + Eq> Metrics<C> {
    pub fn total(&self) -> &Counter {
        &self.total
    }

    pub fn by_status(&self) -> &HashMap<Option<http::StatusCode>, StatusMetrics<C>> {
        &self.by_status
    }
}

impl<C: Hash + Eq> LastUpdate for Metrics<C> {
    fn last_update(&self) -> Instant {
        self.last_update
    }
}

impl<C> Default for StatusMetrics<C>
where
    C: Hash + Eq,
{
    fn default() -> Self {
        Self {
            latency: Histogram::default(),
            by_class: HashMap::default(),
        }
    }
}

#[cfg(feature = "test-util")]
impl<C: Hash + Eq> StatusMetrics<C> {
    pub fn by_class(&self) -> &HashMap<C, ClassMetrics> {
        &self.by_class
    }
}

#[cfg(feature = "test-util")]
impl ClassMetrics {
    pub fn total(&self) -> f64 {
        self.total.value()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn worker_reports_merge_live_request_counts_and_latency_histograms() {
        use super::*;
        use linkerd_metrics::legacy::{Aggregate, FmtLabels};
        use std::fmt;

        #[derive(Clone, Debug, Hash, Eq, PartialEq)]
        struct Label;
        impl FmtLabels for Label {
            fn fmt_labels(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("target=\"same\"")
            }
        }

        #[derive(Clone, Debug, Hash, Eq, PartialEq)]
        struct Class;
        impl FmtLabels for Class {
            fn fmt_labels(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("class=\"success\"")
            }
        }

        let reports = Aggregate::default();
        let mut stores = Vec::new();
        for count in [2, 3] {
            let requests = Requests::<Label, Class>::default();
            let metrics = requests.0.lock().entry(Label).or_default().clone();
            {
                let mut metrics = metrics.lock();
                metrics.total.add(count);
                let status = metrics
                    .by_status
                    .entry(Some(http::StatusCode::OK))
                    .or_default();
                for _ in 0..count {
                    status.latency.add(Duration::from_millis(2));
                }
                status.by_class.entry(Class).or_default().total.add(count);
            }
            let report = requests.into_report(Duration::from_secs(60));
            reports.register(report.clone());
            stores.push((metrics, report));
        }
        // The primary registry and worker registries use the same legacy
        // encoder as the admin endpoint, including cumulative buckets.
        let primary = Requests::<Label, Class>::default().into_report(Duration::from_secs(60));
        let combined = reports.with_primary(primary, true);
        let output = combined.as_display().to_string();
        assert!(output.contains("request_total{target=\"same\"} 5\n"));
        assert!(output
            .contains("response_total{class=\"success\",status_code=\"200\",target=\"same\"} 5\n"));
        assert!(output.contains(
            "response_latency_ms_bucket{le=\"2\",status_code=\"200\",target=\"same\"} 5\n"
        ));
        assert!(
            output.contains("response_latency_ms_count{status_code=\"200\",target=\"same\"} 5\n")
        );
        assert!(
            output.contains("response_latency_ms_sum{status_code=\"200\",target=\"same\"} 10\n")
        );
        assert_eq!(output.matches("# TYPE request_total counter").count(), 1);
        assert_eq!(combined.as_display().to_string(), output);
        stores[0].0.lock().total.incr();
        assert!(stores[1]
            .1
            .as_display()
            .to_string()
            .contains("request_total{target=\"same\"} 3\n"));
        assert!(combined
            .as_display()
            .to_string()
            .contains("request_total{target=\"same\"} 6\n"));
    }

    #[test]
    fn expiry() {
        use linkerd_metrics::legacy::FmtLabels;
        use std::fmt;
        use tokio::time::{Duration, Instant};

        #[derive(Clone, Debug, Hash, Eq, PartialEq)]
        struct Target(usize);
        impl FmtLabels for Target {
            fn fmt_labels(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "n=\"{}\"", self.0)
            }
        }

        #[allow(dead_code)]
        #[derive(Clone, Debug, Hash, Eq, PartialEq)]
        enum Class {
            Good,
            Bad,
        }

        impl FmtLabels for Class {
            fn fmt_labels(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                use std::fmt::Display;
                match self {
                    Class::Good => "class=\"good\"".fmt(f),
                    Class::Bad => "class=\"bad\"".fmt(f),
                }
            }
        }

        let retain_idle_for = Duration::from_secs(1);
        let r = super::Requests::<Target, Class>::default();
        let report = r.clone().into_report(retain_idle_for);
        let mut registry = r.0.lock();

        let before_update = Instant::now();
        let metrics = registry
            .entry(Target(123))
            .or_insert_with(Default::default)
            .clone();
        assert_eq!(registry.len(), 1, "target should be registered");
        let after_update = Instant::now();

        registry.retain_since(after_update);
        assert_eq!(
            registry.len(),
            1,
            "target should not be evicted by time alone"
        );

        drop(metrics);
        registry.retain_since(before_update);
        assert_eq!(
            registry.len(),
            1,
            "target should not be evicted by availability alone"
        );

        registry.retain_since(after_update);
        assert_eq!(
            registry.len(),
            0,
            "target should be evicted by time once the handle is dropped"
        );

        drop((registry, report));
    }
}
