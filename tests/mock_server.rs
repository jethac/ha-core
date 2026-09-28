//! End-to-end tests against a scripted stand-in for Home Assistant's WebSocket API.

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use ha_core::{EntityChange, Error, HaClient, Target};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
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
