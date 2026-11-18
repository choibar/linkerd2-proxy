//! The HTTP/2 client's two send paths, against an `h2` server: hyper's
//! dispatch task (`direct == false`) and direct send (`Connection::direct_send`).

use super::*;
use bytes::Bytes;
use http_body_util::{channel::Channel, BodyExt, Empty};
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::TokioIo;
use tokio::{sync::mpsc, task::JoinHandle};

type Accepted = (
    http::Request<::h2::RecvStream>,
    ::h2::server::SendResponse<Bytes>,
);

struct Test<B> {
    tx: SendRequest<B>,
    /// The client's connection (dispatch) task.
    conn: JoinHandle<()>,
    /// The server's connection task; aborting it closes the connection.
    server: JoinHandle<()>,
    accepted: mpsc::UnboundedReceiver<Accepted>,
}

async fn connect<B>(direct: bool, server: ::h2::server::Builder) -> Test<B>
where
    B: Body + Send + Unpin + 'static,
    B::Data: Send,
    B::Error: Into<Error> + Send + Sync,
{
    let (client_io, server_io) = linkerd_io::duplex(64 * 1024);
    let (accepted_tx, accepted) = mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        let mut conn = server
            .handshake::<_, Bytes>(server_io)
            .await
            .expect("server handshake");
        while let Some(Ok(accepted)) = conn.accept().await {
            let _ = accepted_tx.send(accepted);
        }
    });

    let (mut tx, mut conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake(TokioIo::new(client_io))
        .await
        .expect("client handshake");
    if direct {
        conn.direct_send(&mut tx);
    }
    let conn = tokio::spawn(async move {
        let _ = conn.await;
    });

    Test {
        tx,
        conn,
        server,
        accepted,
    }
}

fn request<B>(body: B) -> http::Request<B> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri("http://example.com/")
        .version(http::Version::HTTP_2)
        .body(body)
        .unwrap()
}

/// A request body of the given chunks, complete.
async fn body_of(chunks: &[&'static str]) -> Channel<Bytes> {
    let (mut tx, body) = Channel::new(chunks.len().max(1));
    for chunk in chunks {
        tx.send_data(Bytes::from_static(chunk.as_bytes()))
            .await
            .unwrap();
    }
    body
}

async fn read_all(mut body: ::h2::RecvStream) -> Result<Vec<u8>, ::h2::Error> {
    let mut all = Vec::new();
    while let Some(data) = body.data().await {
        let data = data?;
        let _ = body.flow_control().release_capacity(data.len());
        all.extend_from_slice(&data);
    }
    Ok(all)
}

fn alive_tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

/// A unary exchange. Direct send spawns no task for the request; the dispatch
/// path has at least its response task alive while the server holds the
/// response.
async fn unary(direct: bool) {
    let mut t = connect::<Channel<Bytes>>(direct, Default::default()).await;

    let body = body_of(&["pi", "ng"]).await;
    let tasks = alive_tasks();
    let rsp = t.tx.send_request(request(body));
    let server = async {
        let (req, mut respond) = t.accepted.recv().await.expect("request");
        assert_eq!(read_all(req.into_body()).await.unwrap(), b"ping");
        let in_flight = alive_tasks();
        let mut send = respond
            .send_response(http::Response::new(()), false)
            .unwrap();
        send.send_data(Bytes::from_static(b"pong"), true).unwrap();
        in_flight
    };
    let (rsp, in_flight) = tokio::join!(rsp, server);

    if direct {
        assert_eq!(in_flight, tasks, "direct send must not spawn tasks");
    } else {
        assert!(in_flight > tasks, "the dispatch path spawns a response task");
    }
    let rsp = rsp.expect("response");
    assert_eq!(rsp.status(), http::StatusCode::OK);
    let body = rsp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"pong");
}

#[tokio::test(flavor = "current_thread")]
async fn unary_dispatch() {
    unary(false).await
}

#[tokio::test(flavor = "current_thread")]
async fn unary_direct() {
    unary(true).await
}

/// The response head arrives while the request body is still open: the rest
/// of the body must reach the server after the caller has its response.
async fn body_outlives_response_head(direct: bool) {
    let mut t = connect::<Channel<Bytes>>(direct, Default::default()).await;

    let (mut body_tx, body) = Channel::new(1);
    let rsp = t.tx.send_request(request(body));
    let server = async {
        let (req, mut respond) = t.accepted.recv().await.expect("request");
        let send = respond
            .send_response(http::Response::new(()), false)
            .unwrap();
        (req.into_body(), send)
    };
    let (rsp, (recv, mut send)) = tokio::join!(rsp, server);
    let rsp = rsp.expect("response");

    for chunk in ["a", "b", "c"] {
        body_tx
            .send_data(Bytes::from_static(chunk.as_bytes()))
            .await
            .expect("request body is still being sent");
    }
    drop(body_tx);
    assert_eq!(read_all(recv).await.unwrap(), b"abc");

    send.send_data(Bytes::from_static(b"done"), true).unwrap();
    let body = rsp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"done");
}

#[tokio::test(flavor = "current_thread")]
async fn body_outlives_response_head_dispatch() {
    body_outlives_response_head(false).await
}

#[tokio::test(flavor = "current_thread")]
async fn body_outlives_response_head_direct() {
    body_outlives_response_head(true).await
}

/// Dropping the response future while the request body is open resets the
/// stream with CANCEL.
async fn cancel_with_open_body(direct: bool) {
    let mut t = connect::<Channel<Bytes>>(direct, Default::default()).await;

    let (_body_tx, body) = Channel::new(1);
    let mut rsp = Box::pin(t.tx.send_request(request(body)));
    let (req, _respond) = tokio::select! {
        biased;
        _ = &mut rsp => panic!("no response was sent"),
        accepted = t.accepted.recv() => accepted.expect("request"),
    };
    drop(rsp);

    let err = read_all(req.into_body())
        .await
        .expect_err("the stream must be reset");
    assert_eq!(err.reason(), Some(Reason::CANCEL));
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_with_open_body_dispatch() {
    cancel_with_open_body(false).await
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_with_open_body_direct() {
    cancel_with_open_body(true).await
}

/// Dropping the response future of a request whose body is complete resets
/// the stream with CANCEL.
async fn cancel_after_body(direct: bool) {
    let mut t = connect::<Empty<Bytes>>(direct, Default::default()).await;

    let mut rsp = Box::pin(t.tx.send_request(request(Empty::new())));
    let (req, mut respond) = tokio::select! {
        biased;
        _ = &mut rsp => panic!("no response was sent"),
        accepted = t.accepted.recv() => accepted.expect("request"),
    };
    assert!(req.body().is_end_stream());
    drop(rsp);

    let reason = futures::future::poll_fn(|cx| respond.poll_reset(cx))
        .await
        .expect("reset");
    assert_eq!(reason, Reason::CANCEL);
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_after_body_dispatch() {
    cancel_after_body(false).await
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_after_body_direct() {
    cancel_after_body(true).await
}

/// A request sent after the connection closed fails as canceled, with no h2
/// reason, on both paths.
async fn send_on_closed_connection(direct: bool) {
    let mut t = connect::<Empty<Bytes>>(direct, Default::default()).await;

    t.server.abort();
    t.conn.await.unwrap();

    let err = t
        .tx
        .send_request(request(Empty::new()))
        .await
        .expect_err("the connection is closed");
    assert!(err.is_canceled(), "{err:?}");
    assert_eq!(crate::HasH2Reason::h2_reason(&err), None);
}

#[tokio::test(flavor = "current_thread")]
async fn send_on_closed_connection_dispatch() {
    send_on_closed_connection(false).await
}

#[tokio::test(flavor = "current_thread")]
async fn send_on_closed_connection_direct() {
    send_on_closed_connection(true).await
}

/// A request in flight when the connection closes fails.
async fn connection_lost_in_flight(direct: bool) {
    let mut t = connect::<Channel<Bytes>>(direct, Default::default()).await;

    let (_body_tx, body) = Channel::new(1);
    let mut rsp = Box::pin(t.tx.send_request(request(body)));
    let accepted = tokio::select! {
        biased;
        _ = &mut rsp => panic!("no response was sent"),
        accepted = t.accepted.recv() => accepted.expect("request"),
    };
    drop(accepted);
    drop(t.accepted);
    t.server.abort();

    rsp.await.expect_err("the connection is closed");
}

#[tokio::test(flavor = "current_thread")]
async fn connection_lost_in_flight_dispatch() {
    connection_lost_in_flight(false).await
}

#[tokio::test(flavor = "current_thread")]
async fn connection_lost_in_flight_direct() {
    connection_lost_in_flight(true).await
}

/// More concurrent requests than the server's stream limit: all complete, the
/// ones over the limit once earlier streams end.
async fn over_stream_limit(direct: bool) {
    let mut server = ::h2::server::Builder::new();
    server.max_concurrent_streams(1);
    let mut t = connect::<Channel<Bytes>>(direct, server).await;

    async fn serve(accepted: &mut mpsc::UnboundedReceiver<Accepted>, n: usize) {
        for _ in 0..n {
            let (req, mut respond) = accepted.recv().await.expect("request");
            assert_eq!(read_all(req.into_body()).await.unwrap(), b"ping");
            respond
                .send_response(http::Response::new(()), true)
                .unwrap();
        }
    }

    // One exchange first, so that the client has the server's settings.
    let rsp = t.tx.send_request(request(body_of(&["ping"]).await));
    let (rsp, ()) = tokio::join!(rsp, serve(&mut t.accepted, 1));
    rsp.expect("response");

    let mut rsps = Vec::new();
    for _ in 0..4 {
        rsps.push(t.tx.send_request(request(body_of(&["ping"]).await)));
    }
    let (rsps, ()) = tokio::join!(futures::future::join_all(rsps), serve(&mut t.accepted, 4));
    for rsp in rsps {
        assert_eq!(rsp.expect("response").status(), http::StatusCode::OK);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn over_stream_limit_dispatch() {
    over_stream_limit(false).await
}

#[tokio::test(flavor = "current_thread")]
async fn over_stream_limit_direct() {
    over_stream_limit(true).await
}
