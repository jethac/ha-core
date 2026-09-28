//! End-to-end tests against a scripted stand-in for Home Assistant's WebSocket API.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use ha_core::{ConnectionState, EntityChange, Error, HaClient, RetryPolicy, Target, TokenProvider};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

const TOKEN: &str = "good-token";

/// Starts a one-connection mock server and returns its base URL.
async fn mock_home_assistant() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        serve(tokio_tungstenite::accept_async(stream).await.unwrap()).await;
    });
    format!("http://{addr}")
}

/// A black hole: accepts the TCP connection, then never speaks.
async fn silent_tcp() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    format!("http://{addr}")
}

/// Completes the WebSocket upgrade and sends `auth_required`, then goes silent.
async fn stalls_after_auth_required() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        send(
            &mut ws,
            json!({"type": "auth_required", "ha_version": "2026.9.0"}),
        )
        .await;
        std::future::pending::<()>().await;
    });
    format!("http://{addr}")
}

async fn serve(mut ws: WebSocketStream<TcpStream>) {
    send(
        &mut ws,
        json!({"type": "auth_required", "ha_version": "2026.9.0"}),
    )
    .await;
    let auth = recv(&mut ws).await.unwrap();
    if auth["access_token"] != TOKEN {
        send(
            &mut ws,
            json!({"type": "auth_invalid", "message": "Invalid access token"}),
        )
        .await;
        return;
    }
    send(
        &mut ws,
        json!({"type": "auth_ok", "ha_version": "2026.9.0"}),
    )
    .await;

    let mut kitchen_on = true;
    let mut entity_sub: Option<Value> = None;
    while let Some(msg) = recv(&mut ws).await {
        let id = msg["id"].clone();
        let ok =
            |result: Value| json!({"id": id, "type": "result", "success": true, "result": result});
        match msg["type"].as_str().unwrap() {
            "supported_features" => send(&mut ws, ok(Value::Null)).await,
            "ping" => send(&mut ws, json!({"id": id, "type": "pong"})).await,
            "get_states" => {
                send(
                    &mut ws,
                    ok(json!([{
                        "entity_id": "light.kitchen",
                        "state": if kitchen_on { "on" } else { "off" },
                        "attributes": {"friendly_name": "Kitchen"},
                        "last_changed": "2026-09-27T10:00:00+00:00",
                        "last_updated": "2026-09-27T10:00:00+00:00",
                        "context": {"id": "c1", "parent_id": null, "user_id": null}
                    }])),
                )
                .await
            }
            "subscribe_entities" => {
                // Coalesced: the ack and the initial snapshot in one frame.
                let snapshot = json!({"id": id, "type": "event", "event": {"a": {
                    "light.kitchen": {"s": "on", "a": {"friendly_name": "Kitchen"}, "c": "c1", "lc": 1_790_503_200.0}
                }}});
                send(&mut ws, json!([ok(Value::Null), snapshot])).await;
                entity_sub = Some(id);
            }
            "call_service" => {
                if msg["domain"] == "light" && msg["service"] == "toggle" {
                    assert_eq!(msg["target"], json!({"entity_id": ["light.kitchen"]}));
                    kitchen_on = !kitchen_on;
                    send(&mut ws, ok(json!({"context": {"id": "c2"}}))).await;
                    if let Some(sub) = &entity_sub {
                        let state = if kitchen_on { "on" } else { "off" };
                        send(&mut ws, json!({"id": sub, "type": "event", "event": {"c": {
                            "light.kitchen": {"+": {"s": state, "c": "c2", "lc": 1_790_503_260.0}}
                        }}}))
                        .await;
                    }
                } else {
                    send(
                        &mut ws,
                        json!({"id": id, "type": "result", "success": false,
                        "error": {"code": "not_found", "message": "Service not found."}}),
                    )
                    .await;
                }
            }
            "unsubscribe_events" => {
                entity_sub = None;
                send(&mut ws, ok(Value::Null)).await;
            }
            other => panic!("mock got unexpected command {other}"),
        }
    }
}

/// Starts a mock server that accepts a new connection each time the previous
/// one drops, and counts how many connections it has seen.
async fn mock_reconnectable_home_assistant() -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let counting = connections.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            counting.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                serve_reconnectable(tokio_tungstenite::accept_async(stream).await.unwrap()).await;
            });
        }
    });
    (format!("http://{addr}"), connections)
}

/// Like `serve`, but answering a `ping` also closes the socket so the client
/// has to reconnect. Each connection carries its own `entity_sub`.
async fn serve_reconnectable(mut ws: WebSocketStream<TcpStream>) {
    send(
        &mut ws,
        json!({"type": "auth_required", "ha_version": "2026.9.0"}),
    )
    .await;
    let auth = recv(&mut ws).await.unwrap();
    if auth["access_token"] != TOKEN {
        send(
            &mut ws,
            json!({"type": "auth_invalid", "message": "Invalid access token"}),
        )
        .await;
        return;
    }
    send(
        &mut ws,
        json!({"type": "auth_ok", "ha_version": "2026.9.0"}),
    )
    .await;
    serve_session(ws).await;
}

/// The post-auth command loop shared by the reconnectable mocks: `ping`
/// answers then drops the socket, `subscribe_entities` replays a snapshot,
/// and `call_service` toggles `light.kitchen`.
async fn serve_session(mut ws: WebSocketStream<TcpStream>) {
    let mut kitchen_on = true;
    let mut entity_sub: Option<Value> = None;
    while let Some(msg) = recv(&mut ws).await {
        let id = msg["id"].clone();
        let ok =
            |result: Value| json!({"id": id, "type": "result", "success": true, "result": result});
        match msg["type"].as_str().unwrap() {
            "supported_features" => send(&mut ws, ok(Value::Null)).await,
            "ping" => {
                send(&mut ws, json!({"id": id, "type": "pong"})).await;
                let _ = ws.close(None).await;
                return;
            }
            "subscribe_entities" => {
                let snapshot = json!({"id": id, "type": "event", "event": {"a": {
                    "light.kitchen": {"s": "on", "a": {"friendly_name": "Kitchen"}, "c": "c1", "lc": 1_790_503_200.0}
                }}});
                send(&mut ws, json!([ok(Value::Null), snapshot])).await;
                entity_sub = Some(id);
            }
            "call_service" => {
                assert_eq!(msg["domain"], "light");
                kitchen_on = !kitchen_on;
                send(&mut ws, ok(json!({"context": {"id": "c2"}}))).await;
                if let Some(sub) = &entity_sub {
                    let state = if kitchen_on { "on" } else { "off" };
                    send(
                        &mut ws,
                        json!({"id": sub, "type": "event", "event": {"c": {
                            "light.kitchen": {"+": {"s": state, "c": "c2", "lc": 1_790_503_260.0}}
                        }}}),
                    )
                    .await;
                }
            }
            "unsubscribe_events" => {
                entity_sub = None;
                send(&mut ws, ok(Value::Null)).await;
            }
            other => panic!("mock got unexpected command {other}"),
        }
    }
}

/// Starts a server that drops the first connection during the auth handshake
/// (answering `auth_required` then closing), then serves normally.
async fn mock_flaky_home_assistant() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        send(&mut ws, json!({"type": "auth_required"})).await;
        drop(ws);
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                serve_reconnectable(tokio_tungstenite::accept_async(stream).await.unwrap()).await;
            });
        }
    });
    format!("http://{addr}")
}

async fn send(ws: &mut WebSocketStream<TcpStream>, msg: Value) {
    ws.send(Message::text(msg.to_string())).await.unwrap();
}

async fn recv(ws: &mut WebSocketStream<TcpStream>) -> Option<Value> {
    loop {
        match ws.next().await? {
            Ok(Message::Text(text)) => return Some(serde_json::from_str(text.as_str()).unwrap()),
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(_) => {}
        }
    }
}

#[tokio::test]
async fn authenticates_and_queries() {
    let client = HaClient::connect(&mock_home_assistant().await, TOKEN)
        .await
        .unwrap();
    assert_eq!(client.ha_version(), "2026.9.0");
    client.ping().await.unwrap();
    let states = client.get_states().await.unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].name(), "Kitchen");
}

#[tokio::test]
async fn connect_times_out_when_tcp_never_answers() {
    let start = Instant::now();
    let result = HaClient::builder()
        .connect_timeout(Duration::from_millis(100))
        .connect(&silent_tcp().await, TOKEN)
        .await;
    assert!(matches!(result, Err(Error::Timeout)));
    assert!(start.elapsed() < Duration::from_secs(10));
}

#[tokio::test]
async fn connect_times_out_when_auth_stalls() {
    let result = HaClient::builder()
        .connect_timeout(Duration::from_millis(100))
        .connect(&stalls_after_auth_required().await, TOKEN)
        .await;
    assert!(matches!(result, Err(Error::Timeout)));
}

#[tokio::test]
async fn rejects_bad_token() {
    let result = HaClient::connect(&mock_home_assistant().await, "wrong").await;
    assert!(matches!(result, Err(Error::AuthInvalid(m)) if m == "Invalid access token"));
}

#[tokio::test]
async fn watcher_sees_snapshot_then_toggle() {
    let client = HaClient::connect(&mock_home_assistant().await, TOKEN)
        .await
        .unwrap();
    let mut watcher = client.watch_entities(None).await.unwrap();

    let snapshot = watcher.next().await.unwrap().unwrap();
    assert!(matches!(&snapshot[..], [EntityChange::Added(s)] if s.is_on()));

    client.toggle("light.kitchen").await.unwrap();
    let batch = watcher.next().await.unwrap().unwrap();
    assert!(matches!(&batch[..], [EntityChange::Updated(s)] if s.state == "off"));
    assert!(!watcher.store().get("light.kitchen").unwrap().is_on());
}

#[tokio::test]
async fn service_errors_surface() {
    let client = HaClient::connect(&mock_home_assistant().await, TOKEN)
        .await
        .unwrap();
    let err = client
        .call_service(
            "light",
            "explode",
            Target::entity("light.kitchen"),
            Value::Null,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Ha { code, .. } if code == "not_found"));
}

#[tokio::test]
async fn explicit_unsubscribe() {
    let client = HaClient::connect(&mock_home_assistant().await, TOKEN)
        .await
        .unwrap();
    let sub = client.subscribe_entities(None).await.unwrap();
    sub.unsubscribe().await.unwrap();
    client.ping().await.unwrap();
}

/// Quick retries for tests instead of the seconds-scale defaults.
/// Serves each connection with `serve_reconnectable`, except the ones whose
/// 0-based index is in `stalls`: those send `auth_required` and go silent.
async fn mock_stalling_home_assistant(stalls: &'static [usize]) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut index = 0;
        while let Ok((stream, _)) = listener.accept().await {
            let stall = stalls.contains(&index);
            index += 1;
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                if stall {
                    send(&mut ws, json!({"type": "auth_required"})).await;
                    std::future::pending::<()>().await;
                }
                serve_reconnectable(ws).await;
            });
        }
    });
    format!("http://{addr}")
}

fn fast_retry() -> RetryPolicy {
    RetryPolicy {
        start: Duration::from_millis(10),
        max: Duration::from_millis(50),
        stable: Duration::ZERO,
    }
}

#[tokio::test]
async fn retry_reconnects_and_resubscribes() {
    let (url, connections) = mock_reconnectable_home_assistant().await;
    let client = HaClient::connect_with_retry(&url, TOKEN, fast_retry())
        .await
        .unwrap();
    let mut watcher = client.watch_entities(None).await.unwrap();
    let snapshot = watcher.next().await.unwrap().unwrap();
    assert!(matches!(&snapshot[..], [EntityChange::Added(s)] if s.is_on()));

    // The mock answers the ping, then drops the socket.
    client.ping().await.unwrap();

    // The resync first reports the old state as removed…
    let resync = watcher.next().await.unwrap().unwrap();
    assert!(
        matches!(&resync[..], [EntityChange::Removed(id)] if id == "light.kitchen"),
        "expected removal of stale state, got {resync:?}"
    );
    // …then the replayed subscription's fresh snapshot arrives.
    let snapshot = watcher.next().await.unwrap().unwrap();
    assert!(matches!(&snapshot[..], [EntityChange::Added(s)] if s.is_on()));
    assert!(connections.load(Ordering::Relaxed) >= 2);

    // Commands and entity events work on the new connection.
    client.toggle("light.kitchen").await.unwrap();
    let batch = watcher.next().await.unwrap().unwrap();
    assert!(matches!(&batch[..], [EntityChange::Updated(s)] if s.state == "off"));
}

#[tokio::test]
async fn retry_retries_the_initial_connect() {
    let client =
        HaClient::connect_with_retry(&mock_flaky_home_assistant().await, TOKEN, fast_retry())
            .await
            .unwrap();
    client.ping().await.unwrap();
}

/// Without a deadline on each attempt, a stalled host would hang the retry
/// loop forever: the failure #2 fixed for `connect`.
#[tokio::test]
async fn retry_gives_up_on_a_stalled_initial_attempt() {
    let url = mock_stalling_home_assistant(&[0]).await;
    let connect = HaClient::builder()
        .connect_timeout(Duration::from_millis(100))
        .connect_with_retry(&url, TOKEN, fast_retry());
    let client = tokio::time::timeout(Duration::from_secs(5), connect)
        .await
        .expect("retry loop stalled on an unresponsive host")
        .unwrap();
    client.ping().await.unwrap();
}

#[tokio::test]
async fn reconnect_gives_up_on_a_stalled_attempt() {
    // Connection 0 serves (and drops after a ping), 1 stalls, 2 serves.
    let url = mock_stalling_home_assistant(&[1]).await;
    let client = HaClient::builder()
        .connect_timeout(Duration::from_millis(100))
        .connect_with_retry(&url, TOKEN, fast_retry())
        .await
        .unwrap();
    let mut watcher = client.watch_entities(None).await.unwrap();
    watcher.next().await.unwrap().unwrap();
    client.ping().await.unwrap();

    let resynced = tokio::time::timeout(Duration::from_secs(5), async {
        watcher.next().await.unwrap().unwrap(); // stale state removed
        watcher.next().await.unwrap().unwrap() // fresh snapshot
    })
    .await
    .expect("supervisor stalled on an unresponsive reconnect");
    assert!(matches!(&resynced[..], [EntityChange::Added(s)] if s.is_on()));
}

/// Like `mock_reconnectable_home_assistant`, but the token it accepts lives in
/// `valid`, which the test can rotate between connections, and every token a
/// connection presents is recorded in `presented`.
async fn mock_rotating_auth_home_assistant(
    initial: &str,
) -> (String, Arc<Mutex<String>>, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let valid = Arc::new(Mutex::new(initial.to_owned()));
    let presented = Arc::new(Mutex::new(Vec::new()));
    tokio::spawn({
        let valid = valid.clone();
        let presented = presented.clone();
        async move {
            while let Ok((stream, _)) = listener.accept().await {
                let valid = valid.clone();
                let presented = presented.clone();
                tokio::spawn(async move {
                    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                    send(
                        &mut ws,
                        json!({"type": "auth_required", "ha_version": "2026.9.0"}),
                    )
                    .await;
                    let auth = recv(&mut ws).await.unwrap();
                    let token = auth["access_token"].as_str().unwrap_or_default().to_owned();
                    presented.lock().unwrap().push(token.clone());
                    if token != *valid.lock().unwrap() {
                        send(
                            &mut ws,
                            json!({"type": "auth_invalid", "message": "Invalid access token"}),
                        )
                        .await;
                        return;
                    }
                    send(
                        &mut ws,
                        json!({"type": "auth_ok", "ha_version": "2026.9.0"}),
                    )
                    .await;
                    serve_session(ws).await;
                });
            }
        }
    });
    (format!("http://{addr}"), valid, presented)
}

/// Waits until `presented` has recorded at least `n` auth tokens.
async fn presented_tokens(presented: &Mutex<Vec<String>>, n: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        {
            let tokens = presented.lock().unwrap();
            if tokens.len() >= n {
                return tokens.clone();
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {n} auth attempts"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A provider is consulted before every attempt, so a token rotated while the
/// client was connected still authenticates the reconnect.
#[tokio::test]
async fn retry_reconnects_with_a_refreshed_token() {
    let (url, valid, presented) = mock_rotating_auth_home_assistant("token-one").await;
    let mut tokens = ["token-one", "token-two"].into_iter();
    let provider = TokenProvider::new(move || {
        let token = tokens.next().unwrap_or("token-two").to_owned();
        async move { Ok::<_, std::convert::Infallible>(token) }
    });
    let client = HaClient::connect_with_retry(&url, provider, fast_retry())
        .await
        .unwrap();

    // The first token expires while the client is connected; the socket drops.
    *valid.lock().unwrap() = "token-two".to_owned();
    client.ping().await.unwrap();

    assert_eq!(
        presented_tokens(&presented, 2).await,
        ["token-one", "token-two"],
        "the reconnect must authenticate with the provider's fresh token"
    );
    client.toggle("light.kitchen").await.unwrap();
}

/// A refused token earns the provider one more consultation on that attempt;
/// a fresh token is tried in place, on the initial connect and on reconnect.
#[tokio::test]
async fn retry_refreshes_a_refused_token_once_per_attempt() {
    let (url, valid, presented) = mock_rotating_auth_home_assistant("fresh-one").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counting = calls.clone();
    let mut tokens = ["stale-one", "fresh-one", "stale-two", "fresh-two"].into_iter();
    let provider = TokenProvider::new(move || {
        counting.fetch_add(1, Ordering::Relaxed);
        let token = tokens.next().unwrap_or("fresh-two").to_owned();
        async move { Ok::<_, std::convert::Infallible>(token) }
    });
    let client = HaClient::connect_with_retry(&url, provider, fast_retry())
        .await
        .unwrap();
    // Initial connect: stale-one refused, fresh-one tried in place.
    assert_eq!(calls.load(Ordering::Relaxed), 2);

    *valid.lock().unwrap() = "fresh-two".to_owned();
    client.ping().await.unwrap(); // answers, then drops the socket

    assert_eq!(
        presented_tokens(&presented, 4).await,
        ["stale-one", "fresh-one", "stale-two", "fresh-two"],
        "reconnect must re-consult the provider once when the token is refused"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 4);
    client.toggle("light.kitchen").await.unwrap();
}

/// A provider with nothing better than the refused token fails the initial
/// connect with `AuthInvalid` instead of retrying it forever.
#[tokio::test]
async fn retry_gives_up_when_provider_cannot_refresh() {
    let (url, _valid, presented) = mock_rotating_auth_home_assistant(TOKEN).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counting = calls.clone();
    let provider = TokenProvider::new(move || {
        counting.fetch_add(1, Ordering::Relaxed);
        async { Ok::<_, std::convert::Infallible>("stale".to_owned()) }
    });
    let result = HaClient::connect_with_retry(&url, provider, fast_retry()).await;
    assert!(matches!(result, Err(Error::AuthInvalid(_))));
    // One attempt: consult, refuse, one more consult, stop.
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert_eq!(presented.lock().unwrap()[..], ["stale"]);
}

/// A fixed `&str` token can never produce a different one: a refused token is
/// still returned immediately rather than retried.
#[tokio::test]
async fn retry_gives_up_on_a_refused_fixed_token() {
    let (url, _valid, presented) = mock_rotating_auth_home_assistant(TOKEN).await;
    let result = HaClient::connect_with_retry(&url, "stale", fast_retry()).await;
    assert!(matches!(result, Err(Error::AuthInvalid(_))));
    // The one re-consultation yields the same token, so no second handshake.
    assert_eq!(presented.lock().unwrap()[..], ["stale"]);
}

/// A provider that fails is a failed attempt, not a refused token: the loop
/// paces the next attempt instead of giving up.
#[tokio::test]
async fn retry_retries_a_failing_provider() {
    let (url, _valid, presented) = mock_rotating_auth_home_assistant("fresh").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counting = calls.clone();
    let provider = TokenProvider::new(move || {
        let n = counting.fetch_add(1, Ordering::Relaxed);
        async move {
            if n == 0 {
                Err(std::io::Error::other("refresh endpoint unreachable"))
            } else {
                Ok("fresh".to_owned())
            }
        }
    });
    let client = HaClient::connect_with_retry(&url, provider, fast_retry())
        .await
        .unwrap();
    client.ping().await.unwrap();
    assert!(calls.load(Ordering::Relaxed) >= 2);
    assert_eq!(presented.lock().unwrap()[..], ["fresh"]);
}

/// A refused reconnect whose provider can only repeat the token stops the
/// client for good — the fixed-token contract survives providers.
#[tokio::test]
async fn reconnect_refusal_with_no_fresh_token_stops_the_client() {
    let (url, valid, presented) = mock_rotating_auth_home_assistant(TOKEN).await;
    let provider =
        TokenProvider::new(|| async { Ok::<_, std::convert::Infallible>(TOKEN.to_owned()) });
    let client = HaClient::connect_with_retry(&url, provider, fast_retry())
        .await
        .unwrap();
    client.ping().await.unwrap(); // answers, then drops the socket
    *valid.lock().unwrap() = "rotated".to_owned(); // TOKEN is refused now

    // The refused reconnect is seen, the provider re-consulted once, and the
    // supervisor exits: nothing is presented again once it has had time to.
    assert_eq!(presented_tokens(&presented, 2).await, [TOKEN, TOKEN]);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        matches!(client.ping().await, Err(Error::Disconnected)),
        "a refused reconnect must stop the client"
    );
    assert_eq!(presented.lock().unwrap()[..], [TOKEN, TOKEN]);
}

/// Waits for a connection state satisfying `pred`, failing after 5s.
async fn await_state(
    states: &mut watch::Receiver<ConnectionState>,
    what: &str,
    pred: impl Fn(&ConnectionState) -> bool,
) {
    let seen = tokio::time::timeout(Duration::from_secs(5), states.wait_for(pred)).await;
    match seen.map(|r| r.map(drop)) {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("state channel closed before {what}"),
        Err(_) => panic!("timed out waiting for {what}: {:?}", states.borrow()),
    }
}

/// A retrying client publishes `Reconnecting` while it re-dials — each failed
/// attempt included — and `Connected` once the new session is up.
#[tokio::test]
async fn state_reports_reconnecting_then_connected() {
    // Connections 1 and 2 stall after auth_required, so two reconnect
    // attempts fail with a timeout before connection 3 serves.
    let url = mock_stalling_home_assistant(&[1, 2]).await;
    let client = HaClient::builder()
        .connect_timeout(Duration::from_millis(100))
        .connect_with_retry(&url, TOKEN, fast_retry())
        .await
        .unwrap();
    let mut states = client.connection_state();
    assert!(matches!(*states.borrow(), ConnectionState::Connected));

    client.ping().await.unwrap(); // answers, then drops the socket

    await_state(&mut states, "Reconnecting", |s| {
        matches!(s, ConnectionState::Reconnecting { .. })
    })
    .await;
    await_state(&mut states, "a failed attempt", |s| {
        matches!(
            s,
            ConnectionState::Reconnecting {
                attempt,
                last_error: Error::Timeout
            } if *attempt >= 1
        )
    })
    .await;
    await_state(&mut states, "Connected", |s| {
        matches!(s, ConnectionState::Connected)
    })
    .await;
    client.toggle("light.kitchen").await.unwrap();
}

/// A refused reconnect publishes the terminal `AuthRejected`.
#[tokio::test]
async fn state_reports_auth_rejected() {
    let (url, valid, _presented) = mock_rotating_auth_home_assistant(TOKEN).await;
    let client = HaClient::connect_with_retry(&url, TOKEN, fast_retry())
        .await
        .unwrap();
    let mut states = client.connection_state();

    client.ping().await.unwrap(); // answers, then drops the socket
    *valid.lock().unwrap() = "rotated".to_owned(); // TOKEN is refused now

    await_state(&mut states, "AuthRejected", |s| {
        matches!(s, ConnectionState::AuthRejected(Error::AuthInvalid(_)))
    })
    .await;
    assert!(
        matches!(client.ping().await, Err(Error::Disconnected)),
        "a refused reconnect must stop the client"
    );
}

/// A provider that keeps failing shows up as `Error::TokenProvider` in
/// `Reconnecting.last_error`, so a caller can treat it as needing sign-in.
#[tokio::test]
async fn state_surfaces_provider_failures() {
    let (url, _connections) = mock_reconnectable_home_assistant().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counting = calls.clone();
    let provider = TokenProvider::new(move || {
        let n = counting.fetch_add(1, Ordering::Relaxed);
        async move {
            if n == 0 {
                Ok(TOKEN.to_owned())
            } else {
                Err(std::io::Error::other("refresh token revoked"))
            }
        }
    });
    let client = HaClient::connect_with_retry(&url, provider, fast_retry())
        .await
        .unwrap();
    let mut states = client.connection_state();

    client.ping().await.unwrap(); // answers, then drops the socket

    await_state(&mut states, "Reconnecting with a provider error", |s| {
        matches!(
            s,
            ConnectionState::Reconnecting {
                attempt,
                last_error: Error::TokenProvider(_)
            } if *attempt >= 1
        )
    })
    .await;
}

/// A non-retrying client reports `Connected`, then `Stopped` when the
/// socket dies.
#[tokio::test]
async fn plain_client_reports_stopped_when_socket_drops() {
    let (url, _connections) = mock_reconnectable_home_assistant().await;
    let client = HaClient::connect(&url, TOKEN).await.unwrap();
    let mut states = client.connection_state();
    assert!(matches!(*states.borrow(), ConnectionState::Connected));

    client.ping().await.unwrap(); // answers, then the server closes

    await_state(&mut states, "Stopped", |s| {
        matches!(s, ConnectionState::Stopped)
    })
    .await;
}

/// A receiver subscribed before the last handle is dropped still sees the
/// terminal `Stopped`.
#[tokio::test]
async fn state_reports_stopped_when_handles_drop() {
    let (url, _connections) = mock_reconnectable_home_assistant().await;
    let client = HaClient::connect_with_retry(&url, TOKEN, fast_retry())
        .await
        .unwrap();
    let mut states = client.connection_state();

    drop(client);

    await_state(&mut states, "Stopped", |s| {
        matches!(s, ConnectionState::Stopped)
    })
    .await;
}
