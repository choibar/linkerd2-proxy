use crate::{client_handle::SetClientHandle, h2, BoxBody, ClientHandle, Variant};
use hyper_util::rt::tokio::TokioExecutor;
use linkerd_error::Error;
use linkerd_http_box::BoxRequest;
use linkerd_io::{self as io, PeerAddr};
use futures::future;
use linkerd_stack::{layer, ExtractParam, NewService, Oneshot, ServiceExt};
use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tower::Service;
use tracing::{debug, Instrument};

#[cfg(test)]
mod tests;

/// Configures HTTP server behavior.
#[derive(Clone, Debug)]
pub struct Params {
    pub version: Variant,
    pub http2: h2::ServerParams,
    pub drain: drain::Watch,
}

// A stack that builds HTTP servers.
#[derive(Clone, Debug)]
pub struct NewServeHttp<X, N> {
    inner: N,
    params: X,
}

/// Serves HTTP connections with an inner service.
#[derive(Clone, Debug)]
pub struct ServeHttp<N> {
    version: Variant,
    http1: hyper::server::conn::http1::Builder,
    http2: hyper::server::conn::http2::Builder<TokioExecutor>,
    /// Raw params, kept so the nghttp2 engine can consume them directly.
    h2_params: h2::ServerParams,
    /// Drive h2 stream futures inline from the connection future (no task
    /// spawn per stream); see `inline_streams()`.
    inline_streams: bool,
    inner: N,
    drain: drain::Watch,
}

// === impl NewServeHttp ===

impl<X: Clone, N> NewServeHttp<X, N> {
    pub fn layer(params: X) -> impl layer::Layer<N, Service = Self> + Clone {
        layer::mk(move |inner| Self::new(params.clone(), inner))
    }

    /// Creates a new `ServeHttp`.
    fn new(params: X, inner: N) -> Self {
        Self { inner, params }
    }
}

impl<T, X, N> NewService<T> for NewServeHttp<X, N>
where
    X: ExtractParam<Params, T>,
    N: NewService<T> + Clone,
{
    type Service = ServeHttp<N::Service>;

    fn new_service(&self, target: T) -> Self::Service {
        let Params {
            version,
            http2: h2,
            drain,
        } = self.params.extract_param(&target);
        let h2_params = h2.clone();
        let h2::ServerParams {
            keep_alive,
            flow_control,
            max_concurrent_streams,
            max_frame_size,
            max_header_list_size,
            max_send_buf_size,
            max_pending_accept_reset_streams,
        } = h2;

        let mut http2 = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
        http2.timer(hyper_util::rt::TokioTimer::new());
        match flow_control {
            None => {}
            Some(h2::FlowControl::Adaptive) => {
                http2.adaptive_window(true);
            }
            Some(h2::FlowControl::Fixed {
                initial_stream_window_size,
                initial_connection_window_size,
            }) => {
                http2
                    .initial_stream_window_size(initial_stream_window_size)
                    .initial_connection_window_size(initial_connection_window_size);
            }
        }

        // Configure HTTP/2 PING frames
        if let Some(h2::KeepAlive { timeout, interval }) = keep_alive {
            http2
                .keep_alive_timeout(timeout)
                .keep_alive_interval(interval);
        }

        http2
            .max_concurrent_streams(max_concurrent_streams)
            .max_frame_size(max_frame_size)
            .max_pending_accept_reset_streams(max_pending_accept_reset_streams);
        if let Some(sz) = max_header_list_size {
            http2.max_header_list_size(sz);
        }
        if let Some(sz) = max_send_buf_size {
            http2.max_send_buf_size(sz);
        }
        if let Some(needed) = crate::selective_h2::needed_set() {
            http2.selective_headers(needed.clone());
            // The relayed response carries the backend's `date` in its raw
            // representations, invisible to hyper's "add date if missing"
            // check on the sparse map; adding one would duplicate it.
            http2.auto_date_header(false);
        }
        let inline_streams = inline_streams();
        http2.inline_streams(inline_streams);
        if inline_streams {
            let (cap, flush) = inline_stream_knobs();
            http2.inline_stream_poll_cap(cap);
            http2.inline_flush_on_complete(flush);
        }

        let mut http1 = hyper::server::conn::http1::Builder::new();
        http1
            .header_read_timeout(None)
            .timer(hyper_util::rt::TokioTimer::new());

        debug!(?version, "Creating HTTP service");
        let inner = self.inner.new_service(target);
        ServeHttp {
            inner,
            version,
            drain,
            http1,
            http2,
            h2_params,
            inline_streams,
        }
    }
}

/// Engine selection, read once at startup.
fn use_nghttp2() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        let on = std::env::var_os("DMESH_NGHTTP2").is_some();
        if on {
            tracing::info!("HTTP/2 server termination: nghttp2 engine");
        }
        on
    })
}

/// Adapts a tower [`Service`] to hyper's `Service` trait **without cloning the
/// tower service on every request**.
///
/// `hyper_util::service::TowerToHyperService::call` is
/// `Oneshot::new(self.service.clone(), req)`. On the outbound stack that clone
/// walks every `BoxCloneSyncService` boundary (a `Box` allocation per
/// boundary), every `WrapFromTarget`/`Distribute` target, and — worst — it
/// hands each request a fresh `MemoOneshotRoute` with an empty memo, so the
/// route memo can never hit across requests and every request rebuilds the
/// route service from the cache (hash + clone + drop of the whole chain).
///
/// hyper drives all of a connection's requests from one task and `call` is
/// synchronous, so interior mutability via `RefCell` is sufficient: the
/// service is polled for readiness and, when immediately ready, called in
/// place. Otherwise (pending or failed) we fall back to exactly the old
/// clone + [`Oneshot`] path, which registers with the real waker and preserves
/// tower's readiness/backpressure semantics unchanged.
struct CallInPlace<S>(RefCell<S>);

impl<S> CallInPlace<S> {
    fn new(inner: S) -> Self {
        Self(RefCell::new(inner))
    }
}

impl<S, Req> hyper::service::Service<Req> for CallInPlace<S>
where
    S: Service<Req> + Clone,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = future::Either<S::Future, Oneshot<S, Req>>;

    fn call(&self, req: Req) -> Self::Future {
        let mut svc = self.0.borrow_mut();
        let mut req = Some(req);

        // hyper dispatches every request accepted in one poll of the connection
        // task from that same poll, so this readiness check (and the readiness
        // checks nested in `call`, e.g. the route memo's) run inside the
        // connection task's tokio coop budget (128 units per poll). Once it is
        // exhausted, budget-aware futures such as `Buffer`'s bounded-channel
        // reserve return a spurious `Pending` that a `LoadShed` above them
        // misreads as "unavailable" and sheds (503 + close). The clone+Oneshot
        // path never saw this because each request was polled in its own
        // spawned task with a fresh budget. So dispatch unconstrained; the
        // request *future* is still driven by hyper's per-stream task under
        // normal budget accounting, exactly as before.
        //
        // A noop waker is fine here: if the service is pending we discard this
        // poll and the fallback `Oneshot` re-registers with the real waker.
        let dispatched = {
            let mut dispatch = tokio::task::unconstrained(std::future::poll_fn(|cx| {
                Poll::Ready(match svc.poll_ready(cx) {
                    Poll::Ready(Ok(())) => Some(svc.call(req.take().expect("polled once"))),
                    _ => None,
                })
            }));
            let mut cx = Context::from_waker(futures::task::noop_waker_ref());
            match Pin::new(&mut dispatch).poll(&mut cx) {
                Poll::Ready(d) => d,
                Poll::Pending => unreachable!("poll_fn always returns Ready"),
            }
        };

        match dispatched {
            Some(fut) => future::Either::Left(fut),
            None => {
                let req = req.take().expect("request not consumed");
                future::Either::Right(svc.clone().oneshot(req))
            }
        }
    }
}

/// Inline-streams selection, read once at startup (`DMESH_INLINE_STREAMS=1`).
///
/// In this mode hyper's h2 server keeps each stream's request/response future
/// in a set owned by the connection and polls it after the connection's I/O,
/// instead of spawning a runtime task per stream. The connection future is
/// then polled with tokio's coop budget disabled (`poll_conn`): tokio 1.49
/// exposes no public per-task budget reset, so a per-stream budget cannot be
/// re-armed from here, and a shared 128-unit budget would otherwise be spent by
/// the first few streams of every poll (spurious `Pending`s, re-queues and
/// wakeups for the rest). Fairness is provided explicitly instead: hyper caps
/// the stream polls per connection poll (`INLINE_STREAM_POLL_CAP`, 1024 —
/// a cap of 64 measured ~16% slower because each round past the cap is a
/// self-wake storm plus a connection re-poll) and self-wakes, so the
/// connection yields to sibling tasks; the connection's own accept loop is
/// bounded by the transport as before.
fn inline_streams() -> bool {
    #[cfg(test)]
    if let Some(on) = tests::INLINE_STREAMS_OVERRIDE.with(|c| c.get()) {
        return on;
    }
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        let on = std::env::var_os("DMESH_INLINE_STREAMS").is_some();
        if on {
            tracing::info!("HTTP/2 server: inline streams (no task per stream)");
        }
        on
    })
}

/// Experiment knobs for inline mode, read once: `DMESH_INLINE_STREAM_CAP`
/// (stream polls per connection poll, default 1024) and `DMESH_INLINE_FLUSH`
/// (`0` = do not re-poll the connection right after stream completions).
fn inline_stream_knobs() -> (usize, bool) {
    use std::sync::OnceLock;
    static KNOBS: OnceLock<(usize, bool)> = OnceLock::new();
    *KNOBS.get_or_init(|| {
        let cap = std::env::var("DMESH_INLINE_STREAM_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024);
        let flush = std::env::var("DMESH_INLINE_FLUSH").map(|v| v != "0").unwrap_or(true);
        tracing::info!(cap, flush, "HTTP/2 server: inline stream knobs");
        (cap, flush)
    })
}

/// Polls the h2 connection future, without tokio's coop budget when streams
/// are driven inline (see [`inline_streams`]).
fn poll_conn<F: Future + Unpin>(
    conn: &mut F,
    inline: bool,
    cx: &mut Context<'_>,
) -> Poll<F::Output> {
    if inline {
        Pin::new(&mut tokio::task::unconstrained(&mut *conn)).poll(cx)
    } else {
        Pin::new(conn).poll(cx)
    }
}

// === impl ServeHttp ===

impl<I, N, S> Service<I> for ServeHttp<N>
where
    I: io::AsyncRead + io::AsyncWrite + PeerAddr + Send + Unpin + 'static,
    N: NewService<ClientHandle, Service = S> + Send + 'static,
    S: Service<http::Request<BoxBody>, Response = http::Response<BoxBody>, Error = Error>
        + Unpin
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = ();
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, io: I) -> Self::Future {
        let version = self.version;
        let drain = self.drain.clone();
        let http1 = self.http1.clone();
        let http2 = self.http2.clone();
        let h2_params = self.h2_params.clone();
        let inline = self.inline_streams;
        let migrate = crate::conn_drain::current();

        let res = io.peer_addr().map(|pa| {
            let (handle, closed) = ClientHandle::new(pa);
            let svc = self.inner.new_service(handle.clone());
            let svc = SetClientHandle::new(handle, svc);
            (svc, closed)
        });

        Box::pin(
            async move {
                let (svc, closed) = res?;
                debug!(?version, "Handling as HTTP");
                match version {
                    Variant::Http1 => {
                        // Enable support for HTTP upgrades (CONNECT and websockets).
                        let svc = linkerd_http_upgrade::upgrade::Service::new(
                            BoxRequest::new(svc),
                            drain.clone(),
                        );
                        let svc = hyper_util::service::TowerToHyperService::new(svc);
                        let io = hyper_util::rt::TokioIo::new(io);
                        let mut conn = http1.serve_connection(io, svc).with_upgrades();

                        tokio::select! {
                            res = &mut conn => {
                                debug!(?res, "The client is shutting down the connection");
                                res?
                            }
                            shutdown = drain.signaled() => {
                                debug!("The process is shutting down the connection");
                                Pin::new(&mut conn).graceful_shutdown();
                                shutdown.release_after(conn).await?;
                            }
                            () = closed => {
                                debug!("The stack is tearing down the connection");
                                Pin::new(&mut conn).graceful_shutdown();
                                conn.await?;
                            }
                            () = crate::conn_drain::signaled(migrate) => {
                                debug!("The connection is draining for migration");
                                Pin::new(&mut conn).graceful_shutdown();
                                conn.await?;
                            }
                        }
                    }

                    // Both engines are compiled in; DMESH_NGHTTP2=1 selects the
                    // nghttp2 one at startup. Same binary either way, so an A/B
                    // measurement has no build-difference confound.
                    Variant::H2 if use_nghttp2() => {
                        // The nghttp2 engine hands the stack `Request<BoxBody>`
                        // directly, so no BoxRequest/TowerToHyperService/TokioIo
                        // adapters are needed. Drain is delivered to the engine
                        // so it can emit GOAWAY and finish in-flight streams.
                        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
                        let drain = drain.clone();
                        tokio::spawn(async move {
                            tokio::select! {
                                _ = drain.signaled() => {}
                                () = closed => {}
                                () = crate::conn_drain::signaled(migrate) => {}
                            }
                            let _ = tx.send(());
                        });
                        linkerd_http_nghttp2::server::serve(
                            io,
                            svc,
                            h2_params,
                            async move {
                                let _ = rx.await;
                            },
                        )
                        .await
                        .map_err(Error::from)?;
                    }

                    Variant::H2 => {
                        let svc = CallInPlace::new(BoxRequest::new(svc));
                        let io = hyper_util::rt::TokioIo::new(io);
                        let mut conn = http2.serve_connection(io, svc);

                        tokio::select! {
                            res = std::future::poll_fn(|cx| poll_conn(&mut conn, inline, cx)) => {
                                debug!(?res, "The client is shutting down the connection");
                                res?
                            }
                            shutdown = drain.signaled() => {
                                debug!("The process is shutting down the connection");
                                Pin::new(&mut conn).graceful_shutdown();
                                shutdown
                                    .release_after(std::future::poll_fn(|cx| {
                                        poll_conn(&mut conn, inline, cx)
                                    }))
                                    .await?;
                            }
                            () = closed => {
                                debug!("The stack is tearing down the connection");
                                Pin::new(&mut conn).graceful_shutdown();
                                std::future::poll_fn(|cx| poll_conn(&mut conn, inline, cx)).await?;
                            }
                            () = crate::conn_drain::signaled(migrate) => {
                                debug!("The connection is draining for migration");
                                Pin::new(&mut conn).graceful_shutdown();
                                std::future::poll_fn(|cx| poll_conn(&mut conn, inline, cx)).await?;
                            }
                        }
                    }
                }
                Ok(())
            }
            .instrument(tracing::debug_span!("http").or_current()),
        )
    }
}
