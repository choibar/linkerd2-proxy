//! The main entrypoint for the proxy.

#![deny(rust_2018_idioms, clippy::disallowed_methods, clippy::disallowed_types)]
#![forbid(unsafe_code)]
#![recursion_limit = "256"]

use linkerd_app::{trace, BindTcp, Config, BUILD_INFO};
use linkerd_signal as signal;
use tokio::{sync::mpsc, time};
use tracing::{debug, info, warn};

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    target_env = "gnu"
))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod rt;

const EX_USAGE: i32 = 64;

/// Initialize linkerd's fmt logging subscriber, exiting on a bad config.
fn init_linkerd_trace() -> trace::Handle {
    match trace::Settings::from_env().init() {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Invalid logging configuration: {e}");
            std::process::exit(EX_USAGE);
        }
    }
}

fn main() {
    // tokio-console is gated behind the `tokio-console` cargo feature, which
    // is OFF by default because the tokio "tracing" runtime instrumentation it
    // requires costs throughput on the hot path (~2.4x at warn) even with no
    // subscriber. Build `--features tokio-console` for a diagnostic session
    // only. When set, DMESH_TOKIO_CONSOLE takes the single global tracing
    // dispatcher (console runs its own background runtime, so starting it here
    // — before ours — is fine) and the app gets a disabled trace handle: you
    // get console, not proxy logs.
    #[cfg(feature = "tokio-console")]
    let trace = if std::env::var_os("DMESH_TOKIO_CONSOLE").is_some() {
        console_subscriber::init();
        trace::Handle::disabled()
    } else {
        init_linkerd_trace()
    };
    #[cfg(not(feature = "tokio-console"))]
    let trace = init_linkerd_trace();

    info!(
        "{profile} {version} ({sha}) by {vendor} on {date}",
        date = BUILD_INFO.date,
        sha = BUILD_INFO.git_sha,
        version = BUILD_INFO.version,
        profile = BUILD_INFO.profile,
        vendor = BUILD_INFO.vendor,
    );

    linkerd_rustls::install_default_provider();

    // Stage 1 (synchronous, pre-runtime): open the DOCA device and start the
    // dmesh comch server. The async driver is spawned inside the runtime below.
    #[cfg(feature = "doca")]
    let worker_cores = dmesh_doca::allowed_worker_cores().expect("read worker CPU affinity");
    #[cfg(feature = "doca")]
    let dmesh_doca = {
        if let Err(error) = dmesh_doca::backend::init_routes() {
            eprintln!("Invalid DMA routes: {error}");
            std::process::exit(EX_USAGE);
        }
        match dmesh_doca::initialize() {
            Ok(report) => info!("{}", report.log_summary()),
            Err(error) => {
                eprintln!("DOCA initialization failure: {error}");
                std::process::exit(1);
            }
        }
        let dev_pci_addr = match std::env::var("LINKERD2_PROXY_DOCA_DEV_PCI_ADDR") {
            Ok(addr) => addr,
            Err(error) => {
                eprintln!("Invalid DOCA configuration: LINKERD2_PROXY_DOCA_DEV_PCI_ADDR: {error}");
                std::process::exit(EX_USAGE);
            }
        };
        let rep_pci_addr = match std::env::var("LINKERD2_PROXY_DOCA_REP_PCI_ADDR") {
            Ok(addr) => addr,
            Err(error) => {
                eprintln!("Invalid DOCA configuration: LINKERD2_PROXY_DOCA_REP_PCI_ADDR: {error}");
                std::process::exit(EX_USAGE);
            }
        };
        let server_name = std::env::var("LINKERD2_PROXY_DOCA_SERVER_NAME")
            .unwrap_or_else(|_| "DPUMesh0".to_string());
        // Comch aliases are control endpoints, not worker placement hints.
        // A single dispatcher assigns each native flow to a data worker.
        let num_workers: usize = std::env::var("DMESH_NUM_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(1);
        if std::env::var_os("DMESH_SHARDED").is_some() && num_workers > worker_cores.len() {
            eprintln!("DMESH_NUM_WORKERS exceeds CPUs allowed by process affinity");
            std::process::exit(EX_USAGE);
        }
        match dmesh_doca::DmeshDoca::initialize_group(
            &dev_pci_addr,
            &rep_pci_addr,
            &server_name,
            num_workers,
        ) {
            Ok(workers) => workers,
            Err(error) => {
                eprintln!("DOCA dispatcher initialization failure: {error}");
                std::process::exit(1);
            }
        }
    };

    let mut metrics = linkerd_metrics::prom::Registry::default();

    // Load configuration from the environment without binding ports.
    let config = match Config::try_from_env() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Invalid configuration: {e}");
            std::process::exit(EX_USAGE);
        }
    };

    // Builds a runtime with the appropriate number of cores:
    // `LINKERD2_PROXY_CORES` env or the number of available CPUs (as provided
    // by cgroups, when possible).
    rt::build().block_on(async move {
        // Spawn a task to run in the background, exporting runtime metrics at a regular interval.
        rt::spawn_metrics_exporter(&mut metrics);

        // Stage 2: drive the dmesh control/data paths event-driven (AsyncFd on
        // the DOCA progress-engine fds). The event stream + registrar are handed
        // to the app's dmesh acceptor (spawned after the app is built) so
        // DMA-received connections flow through the real outbound stack.
        // DMESH_SHARDED=1: shared-nothing worker threads. Each worker gets a
        // dedicated OS thread pinned to its own core running a current_thread
        // runtime that hosts the driver AND every flow it serves — no
        // work-stealing, no cross-core task migration (mirrors dpu_worker.c).
        let dmesh_sharded = std::env::var_os("DMESH_SHARDED").is_some();
        #[cfg(feature = "doca")]
        let mut dmesh_acceptors = Vec::new();
        #[cfg(feature = "doca")]
        let mut dmesh_shards = Vec::new();
        #[cfg(feature = "doca")]
        for (i, doca) in dmesh_doca.into_iter().enumerate() {
            let (dmesh_tx, dmesh_rx) = mpsc::unbounded_channel();
            let (driver, registrar) = dmesh_doca::Driver::new(doca, dmesh_tx);
            if dmesh_sharded {
                dmesh_shards.push((i, driver, dmesh_rx, registrar));
            } else {
                dmesh_doca::backend::register_worker(i);
                tokio::spawn(async move {
                    match driver.run().await {
                        Ok(()) => warn!(worker = i, "dmesh driver exited"),
                        Err(error) => warn!(worker = i, %error, "dmesh driver failed"),
                    }
                });
                dmesh_acceptors.push((i, dmesh_rx, registrar));
            }
        }

        let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();
        let shutdown_grace_period = config.shutdown_grace_period;

        let bind_in = BindTcp::with_orig_dst();
        let bind_out = BindTcp::dual_with_orig_dst();
        let app = match config
            .build(
                bind_in,
                bind_out,
                BindTcp::default(),
                shutdown_tx,
                trace,
                metrics,
            )
            .await
        {
            Ok(app) => app,
            Err(e) => {
                eprintln!("Initialization failure: {e}");
                std::process::exit(1);
            }
        };

        // Drive DMA-received connections through the outbound stack.
        #[cfg(feature = "doca")]
        for (i, dmesh_rx, registrar) in dmesh_acceptors {
            app.spawn_dmesh(i, dmesh_rx, registrar);
        }

        // Sharded mode: one pinned thread + current_thread runtime per worker.
        // Select distinct online CPUs inside the launch affinity, highest first.
        #[cfg(feature = "doca")]
        for (i, driver, dmesh_rx, registrar) in dmesh_shards {
            let serve = app.dmesh_serve_future(i, dmesh_rx, registrar);
            let core = worker_cores[i];
            std::thread::Builder::new()
                .name(format!("dmesh-shard-{i}"))
                .spawn(move || {
                    if !dmesh_doca::pin_current_thread_to_core(core) {
                        warn!(worker = i, core, "dmesh shard: failed to pin core");
                    }
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("dmesh shard runtime");
                    rt.block_on(async move {
                        dmesh_doca::backend::register_worker(i);
                        info!(
                            worker = i,
                            core, "dmesh shard running (pinned current_thread runtime)"
                        );
                        tokio::spawn(async move {
                            match driver.run().await {
                                Ok(()) => warn!(worker = i, "dmesh driver exited"),
                                Err(error) => warn!(worker = i, %error, "dmesh driver failed"),
                            }
                        });
                        serve.await;
                    });
                })
                .expect("spawn dmesh shard thread");
        }

        info!("Admin interface on {}", app.admin_addr());
        info!("Inbound interface on {}", app.inbound_addr());
        info!("Outbound interface on {}", app.outbound_addr());
        if let Some(addr) = app.outbound_addr_additional() {
            info!("Outbound interface on {addr}");
        }

        match app.tap_addr() {
            None => info!("Tap DISABLED"),
            Some(addr) => info!("Tap interface on {}", addr),
        }

        // TODO distinguish ServerName and Identity.
        info!("SNI is {}", app.local_server_name());
        info!("Local identity is {}", app.local_tls_id());

        let dst_addr = app.dst_addr();
        match dst_addr.identity.value() {
            None => info!("Destinations resolved via {}", dst_addr.addr),
            Some(tls) => info!(
                "Destinations resolved via {} ({})",
                dst_addr.addr, tls.server_id
            ),
        }

        if let Some(tracing) = app.tracing_addr() {
            match tracing.identity.value() {
                None => info!("Tracing collector at {}", tracing.addr),
                Some(tls) => {
                    info!("Tracing collector at {} ({})", tracing.addr, tls.server_id)
                }
            }
        }

        let drain = app.spawn();
        tokio::select! {
            _ = signal::shutdown() => {
                info!("Received shutdown signal");
            }
            _ = shutdown_rx.recv() => {
                info!("Received shutdown via admin interface");
            }
        }
        match time::timeout(shutdown_grace_period, drain.drain()).await {
            Ok(()) => debug!("Shutdown completed gracefully"),
            Err(_) => warn!(
                "Graceful shutdown did not complete in {shutdown_grace_period:?}, terminating now"
            ),
        }
    });
}
