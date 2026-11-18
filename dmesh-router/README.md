# dmesh-router

A minimal HTTP/2 router on the DPUMesh DMA datapath — the same position in the
system as the DMA-enabled `linkerd2-proxy`, with none of its L7 machinery.

It runs on the DPU, accepts connections that arrive over PCIe DMA from the host,
terminates HTTP/2 on them with `hyper::server::conn::http2`, and forwards each
request to a backend the host provides over a second DMA channel. There is **no
tower stack**: no protocol detection, discovery, load balancing, service
profiles, policy, mTLS or telemetry. Routing is a hash lookup; the request
handler is one `async fn` behind `hyper::service::service_fn`.

That makes it the floor against which the proxy's L7 cost is measured: identical
transport (`dmesh-doca`, the same `Driver` and `DmeshIo`), identical hyper/h2
versions (the workspace's vendored `hyper` 1.10.1 / `h2` 0.4), identical
allocator and release profile.

## Shape

```
h2load (host library) ──forward DMA──▶ ┌──────────────────────┐
                                        │ dmesh-router (DPU)   │
                      ◀──reverse DMA── │  hyper h2 server     │
                                        │      ↓ route(dst)    │
nginx (host library)  ◀──reverse DMA── │  hyper client        │
                                        └──────────────────────┘
```

A `DmeshEvent::ConnReady` carrying `is_backend` becomes a hyper *client* over
that channel, registered under the flow's destination. Any other `ConnReady`
becomes a hyper h2 *server*; each stream is routed to a registered channel.
The host side is the DPUMesh host library; the router needs no C-side changes.

## Configuration (environment only)

| Variable | Default | Meaning |
| --- | --- | --- |
| `DMESH_ROUTER_DEV_PCI` | `03:00.1` | DOCA device on the DPU |
| `DMESH_ROUTER_REP_PCI` | `94:00.1` | Representor of the host function |
| `DMESH_ROUTER_SERVER` | `DPUMesh0` | Comch server name (`host_worker.c` dials `DPUMesh<idx % -d>`) |
| `DMESH_ROUTER_CORES` | `1` | 1 → current-thread scheduler; >1 → multi-thread |
| `DMESH_ROUTER_BACKEND_PROTO` | `http1` | `http1` or `h2` — protocol spoken to the backend |
| `DMESH_ROUTER_ROUTES` | *(empty)* | `authority=ip:port,...`; overrides the flow destination |
| `DMESH_ROUTER_DEFAULT_BACKEND` | *(unset)* | Fallback when the destination has no channel |
| `DMESH_ROUTER_BACKEND_WAIT_MS` | `5000` | How long a request waits for its backend channel |
| `DMESH_ROUTER_MAX_STREAMS` | `1000` | h2 `MAX_CONCURRENT_STREAMS` (must cover h2load `-m`) |
| `DMESH_ROUTER_LOG` | `info` | `EnvFilter` directive |
| `DMESH_BUSY_POLL` | *(unset = busy-poll)* | Read by the driver: busy-polls the progress engines by default; `0` sleeps on their notification fds instead (event-driven) |

Routing order for a request: `DMESH_ROUTER_ROUTES[authority]` → the connection's
flow destination (the service address the host's flow carries, the
`SO_ORIGINAL_DST` analogue) → `DMESH_ROUTER_DEFAULT_BACKEND`. No registered
channel after the wait → `503`; a failed backend request → `502`.

`http1` is the default because it matches what the proxy does today (the
destination profile carries no `h2` protocol hint). One HTTP/1.1 channel carries
one request at a time, so it becomes the bottleneck under concurrency — scale it
by running several backend processes for the same service (they round-robin),
or use `h2`, where one channel multiplexes every stream.

## Measured (2026-08-12, testbed, 1 core, nginx on `127.0.0.1:8086`)

`h2load -c1 -m100 -n20000`, one client channel and one backend channel:

| backend leg | req/s | mean request | note |
| --- | --- | --- | --- |
| `h2` | **30,138** | 3.30 ms | 20000/20000 → 200; nginx has `listen 8086 http2` |
| `http1` | 5,730 | 17.41 ms | serialized on one channel — backend-bound, not router-bound |

For reference the DMA-enabled `linkerd2-proxy` measures ~16.6k req/s on this
datapath, so the h2 configuration is the meaningful floor. Host-view RTT
(`fwd-commit → rev-arrive`, printed by the bridge) was 123 µs at `-m10`.
Repeat h2 runs: 30,138 and 31,483 req/s.

`../../dmesh-router-cpp/` is the same router in C++ on libnghttp2, driving the
identical datapath: ~100k req/s in the same configuration (after its no-copy
header + jemalloc optimizations; ~68k before).

## CPU profile (2026-08-12, perf, 300k requests, flat self-time, h2 backend leg)

Sampled at ~30.6k req/s over the whole run. Self-time by category:

| area | self % | top symbols |
| --- | --- | --- |
| `h2` crate (HPACK + framing + stream state machine) | **36%** | `HeaderBlock::into_encoding` 4.0%, `hpack::Decoder::try_decode_string` 2.8%, `HeaderBlock::load` 2.0%, `recv_headers` 1.7%, `Prioritize::poll_complete` 1.4% |
| atomics (Arc refcounts, wakers) | **13%** | `__aarch64_cas4_acq` 2.9%, `ldadd8_rel` 2.8%, `swp4_rel` 2.6% |
| tokio runtime (task poll/scheduling) | 9% | `task::core::Core::poll` 2.5% |
| hyper + http (request/response assembly, HeaderMap) | 8% | `H2Stream::poll` 3.3%, `HeaderMap::try_append2` |
| memcpy/strings | 8% | `__memcpy_generic` 7.7% (largest single symbol) |
| allocator (jemalloc) | 4% | `_rjem_malloc` |
| this crate (driver/accept/proxy) | 3% | driver loop closure 0.9% |
| other (bytes/slab/futures glue, kernel) | 18% | |

Read against the C++ router's profile (see `../../dmesh-router-cpp/README.md`),
this explains the ~3× throughput gap:

- The h2 engine is the top cost in both (36% here, 43% nghttp2) — that part is
  intrinsic to h2 termination. But hyper layers request assembly on top of the
  `h2` crate (~45% combined), where nghttp2 is one C library.
- **~22% of cycles are Rust-stack-only: atomics 13% + tokio 9%.** Even on a
  current_thread runtime, Arc refcounts, waker registration and the task queue
  all run atomic ops; the C++ router's callbacks feed the event loop directly
  and this layer simply does not exist there.
- jemalloc already works well here (allocator 4% vs 28% glibc malloc in the
  unoptimized C++ router) — nothing to gain on that axis.
- This crate's own logic is 3%: the bottleneck is the hyper/h2/tokio stack's
  layering, not the router code, so there is little headroom short of replacing
  the stack (which is exactly what `dmesh-router-cpp` demonstrates).

Raw data: recorded with `perf record -F 999` (no call graph — flat) against the
running router PID during a sustained h2load run.

## Build

```bash
ninja -C ../../src/transport/build     # transport archives + dpa_kernel.a must exist; dmesh-doca links them
cargo build --release -p dmesh-router
```

## Run (testbed)

DPU:

```bash
./target/release/dmesh-router
```

On the host (`192.168.100.1`), the backend and the load generator run over
the DPUMesh host library: nginx under the preload façade with
`DPUMESH_SERVICE` and `DPUMESH_PORT`, h2load under it with `DPUMESH_TARGETS`
naming that service (DPUMesh `examples/README.md`, preload). The service's
address is the routing key.

The DPU process must be restarted between runs: tearing down a client
connection currently segfaults the shared DOCA datapath (see CLAUDE.md
gotchas), so `dmesh-router` exits when h2load disconnects.

Latency probe in the datapath: `[dmesh-lat] DPU-internal` (driver, DPU side).

## Tests

```bash
cargo test -p dmesh-router
```

Routing, header rewriting per backend protocol, late-backend waiting, 503 on an
unrouted destination and round-robin across channels are covered over
`tokio::io::duplex` pipes — everything except the DMA transport itself, so they
run anywhere.
