use futures::{stream, StreamExt as _};
use linkerd2_proxy_api::{destination, destination::destination_server};
use std::{convert::TryInto, net::SocketAddr, pin::Pin};
use tokio_stream::Stream;
use tonic::{transport::Server, Request, Response, Status};

const DEFAULT_ADDR: &str = "127.0.0.1:8089";
const DEFAULT_BACKEND: &str = "127.0.0.1:8086";

type GetStream =
    Pin<Box<dyn Stream<Item = Result<destination::Update, Status>> + Send + Sync + 'static>>;
type GetProfileStream = Pin<
    Box<dyn Stream<Item = Result<destination::DestinationProfile, Status>> + Send + Sync + 'static>,
>;

#[derive(Clone, Debug)]
struct Destination {
    backend: SocketAddr,
    routes: Option<tokio::sync::watch::Receiver<std::sync::Arc<dmesh_routing::Manifest>>>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("MOCK_DESTINATION_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse::<SocketAddr>()?;
    let backend = std::env::var("MOCK_DESTINATION_BACKEND")
        .unwrap_or_else(|_| DEFAULT_BACKEND.to_string())
        .parse::<SocketAddr>()?;

    eprintln!("mock destination serving on {addr}");
    eprintln!("destination endpoint: {backend}");

    Server::builder()
        .add_service(destination_server::DestinationServer::new(Destination {
            backend,
            routes: dmesh_routing::from_env().map_err(std::io::Error::other)?,
        }))
        .serve(addr)
        .await?;

    Ok(())
}

#[tonic::async_trait]
impl destination_server::Destination for Destination {
    type GetStream = GetStream;
    type GetProfileStream = GetProfileStream;

    async fn get(
        &self,
        req: Request<destination::GetDestination>,
    ) -> Result<Response<Self::GetStream>, Status> {
        let req = req.into_inner();
        if let Some(routes) = &self.routes {
            if routes.borrow().by_path(&req.path).is_none() {
                return Err(Status::not_found("unknown DMA service"));
            }
            let mut previous = std::collections::BTreeSet::<SocketAddr>::new();
            let stream =
                tokio_stream::wrappers::WatchStream::new(routes.clone()).flat_map(move |m| {
                    let next: std::collections::BTreeSet<_> = m
                        .by_path(&req.path)
                        .into_iter()
                        .flat_map(|s| &s.endpoints)
                        .filter(|e| e.enabled)
                        .map(|e| e.dma)
                        .collect();
                    let removed: Vec<_> = previous
                        .difference(&next)
                        .map(|a| (*a).try_into().unwrap())
                        .collect();
                    let added: Vec<_> = next
                        .difference(&previous)
                        .map(|a| destination::WeightedAddr {
                            addr: Some((*a).try_into().unwrap()),
                            weight: 1,
                            protocol_hint: Some(destination::ProtocolHint {
                                protocol: Some(destination::protocol_hint::Protocol::H2(
                                    destination::protocol_hint::H2 {},
                                )),
                                opaque_transport: None,
                            }),
                            ..Default::default()
                        })
                        .collect();
                    let mut updates = Vec::new();
                    if !removed.is_empty() {
                        updates.push(destination::update::Update::Remove(destination::AddrSet {
                            addrs: removed,
                        }));
                    }
                    if !added.is_empty() {
                        updates.push(destination::update::Update::Add(
                            destination::WeightedAddrSet {
                                addrs: added,
                                metric_labels: Default::default(),
                            },
                        ));
                    }
                    if next.is_empty() {
                        updates.push(destination::update::Update::NoEndpoints(
                            destination::NoEndpoints { exists: true },
                        ));
                    }
                    previous = next;
                    stream::iter(
                        updates
                            .into_iter()
                            .map(|u| Ok(destination::Update { update: Some(u) })),
                    )
                });
            return Ok(Response::new(Box::pin(stream)));
        }
        eprintln!("destination get path={} backend={}", req.path, self.backend);

        let update = destination::Update {
            update: Some(destination::update::Update::Add(
                destination::WeightedAddrSet {
                    addrs: vec![destination::WeightedAddr {
                        addr: Some(self.backend.try_into().map_err(|error| {
                            Status::internal(format!("invalid backend address: {error}"))
                        })?),
                        weight: 1,
                        metric_labels: Default::default(),
                        protocol_hint: None,
                        tls_identity: None,
                        authority_override: None,
                        http2: None,
                        resource_ref: None,
                    }],
                    metric_labels: Default::default(),
                },
            )),
        };
        let stream = stream::once(async move { Ok(update) }).chain(stream::pending());
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_profile(
        &self,
        req: Request<destination::GetDestination>,
    ) -> Result<Response<Self::GetProfileStream>, Status> {
        let req = req.into_inner();
        eprintln!("destination get_profile path={}", req.path);
        // MOCK_DEST_OPAQUE=1 marks the destination opaque so the outbound stack
        // byte-forwards (L4) instead of terminating HTTP/2 — used to measure the
        // proxy's cost with the h2 stack bypassed (h2load<->nginx h2 end-to-end).
        let profile = destination::DestinationProfile {
            fully_qualified_name: req.path,
            opaque_protocol: std::env::var("MOCK_DEST_OPAQUE").is_ok(),
            ..Default::default()
        };
        let stream = stream::once(async move { Ok(profile) }).chain(stream::pending());
        Ok(Response::new(Box::pin(stream)))
    }
}
