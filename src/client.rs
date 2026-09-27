//! Connection, authentication, and request/subscription routing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::entities::{EntityChange, EntityState, EntityStore, domain_of};
use crate::protocol::Incoming;
use crate::registry::{AreaEntry, DeviceEntry, EntityRegistryEntry};
use crate::{Error, Result};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Handle to one authenticated Home Assistant connection.
///
/// Cheap to clone; the connection closes once every clone (and every
/// [`Subscription`]) has been dropped. If the connection drops, pending and
/// future calls fail with [`Error::Disconnected`]; reconnecting is up to the caller.
#[derive(Clone)]
pub struct HaClient {
    shared: Arc<Shared>,
}

struct Shared {
    next_id: AtomicU64,
    out: mpsc::UnboundedSender<Message>,
    routes: Arc<Mutex<Routes>>,
    ha_version: String,
}

#[derive(Default)]
struct Routes {
    closed: bool,
    pending: HashMap<u64, oneshot::Sender<Result<Value>>>,
    subscriptions: HashMap<u64, mpsc::UnboundedSender<Value>>,
}

impl HaClient {
    /// Connects and authenticates with a long-lived access token.
    ///
    /// `url` may be the instance's base URL (`http://homeassistant.local:8123`)
    /// or the full WebSocket endpoint (`wss://…/api/websocket`).
    pub async fn connect(url: &str, access_token: &str) -> Result<Self> {
        let url = websocket_url(url)?;
        let (mut ws, _) = tokio_tungstenite::connect_async(url.as_str()).await?;

        match next_incoming(&mut ws).await? {
            Incoming::AuthRequired { .. } => {}
            other => {
                return Err(Error::Protocol(format!(
                    "expected auth_required, got {other:?}"
                )));
            }
        }
        let auth = json!({"type": "auth", "access_token": access_token});
        ws.send(Message::text(auth.to_string())).await?;
        let ha_version = match next_incoming(&mut ws).await? {
            Incoming::AuthOk { ha_version } => ha_version.unwrap_or_default(),
            Incoming::AuthInvalid { message } => {
                return Err(Error::AuthInvalid(message.unwrap_or_default()));
            }
            other => return Err(Error::Protocol(format!("expected auth_ok, got {other:?}"))),
        };

        let (out, out_rx) = mpsc::unbounded_channel();
        let routes = Arc::new(Mutex::new(Routes::default()));
        tokio::spawn(run_connection(ws, out_rx, routes.clone()));
        let client = HaClient {
            shared: Arc::new(Shared {
                next_id: AtomicU64::new(1),
                out,
                routes,
                ha_version,
            }),
        };

        // Lets Home Assistant batch messages into JSON arrays; older versions reject it.
        let features = json!({"type": "supported_features", "features": {"coalesce_messages": 1}});
        if let Err(e) = client.command(features).await {
            tracing::debug!("message coalescing unavailable: {e}");
        }
        Ok(client)
    }

    pub fn ha_version(&self) -> &str {
        &self.shared.ha_version
    }

    /// Sends a raw command and returns its `result`. The `id` field is filled in.
    pub async fn command(&self, msg: Value) -> Result<Value> {
        let (id, msg) = self.stamp(msg)?;
        let (tx, rx) = oneshot::channel();
        {
            let mut routes = self.shared.routes.lock().unwrap();
            if routes.closed {
                return Err(Error::Disconnected);
            }
            routes.pending.insert(id, tx);
        }
        self.send(&msg)?;
        rx.await.map_err(|_| Error::Disconnected)?
    }

    /// Sends a raw subscription command; its events arrive on the returned [`Subscription`].
    pub async fn subscribe(&self, msg: Value) -> Result<Subscription> {
        let (id, msg) = self.stamp(msg)?;
        let (ack_tx, ack_rx) = oneshot::channel();
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        {
            let mut routes = self.shared.routes.lock().unwrap();
            if routes.closed {
                return Err(Error::Disconnected);
            }
            routes.pending.insert(id, ack_tx);
            routes.subscriptions.insert(id, events_tx);
        }
        self.send(&msg)?;
        match ack_rx.await.map_err(|_| Error::Disconnected)? {
            Ok(_) => Ok(Subscription {
                id,
                events: events_rx,
                client: self.clone(),
                active: true,
            }),
            Err(e) => {
                self.shared.routes.lock().unwrap().subscriptions.remove(&id);
                Err(e)
            }
        }
    }

    pub async fn ping(&self) -> Result<()> {
        self.command(json!({"type": "ping"})).await.map(drop)
    }

    pub async fn get_states(&self) -> Result<Vec<EntityState>> {
        Ok(serde_json::from_value(
            self.command(json!({"type": "get_states"})).await?,
        )?)
    }

    pub async fn get_config(&self) -> Result<Value> {
        self.command(json!({"type": "get_config"})).await
    }

    pub async fn get_services(&self) -> Result<Value> {
        self.command(json!({"type": "get_services"})).await
    }

    pub async fn areas(&self) -> Result<Vec<AreaEntry>> {
        Ok(serde_json::from_value(
            self.command(json!({"type": "config/area_registry/list"}))
                .await?,
        )?)
    }

    pub async fn devices(&self) -> Result<Vec<DeviceEntry>> {
        Ok(serde_json::from_value(
            self.command(json!({"type": "config/device_registry/list"}))
                .await?,
        )?)
    }

    pub async fn entity_registry(&self) -> Result<Vec<EntityRegistryEntry>> {
        Ok(serde_json::from_value(
            self.command(json!({"type": "config/entity_registry/list"}))
                .await?,
        )?)
    }

    /// Calls `domain.service` on `target` with extra `service_data` (`Value::Null` for none).
    pub async fn call_service(
        &self,
        domain: &str,
        service: &str,
        target: Target,
        service_data: Value,
    ) -> Result<Value> {
        let mut msg = json!({"type": "call_service", "domain": domain, "service": service});
        if !target.is_empty() {
            msg["target"] = serde_json::to_value(&target)?;
        }
        if !service_data.is_null() {
            msg["service_data"] = service_data;
        }
        self.command(msg).await
    }

    /// Calls `<entity's domain>.turn_on`, e.g. `light.turn_on` with `{"brightness_pct": 40}`.
    pub async fn turn_on(&self, entity_id: &str, service_data: Value) -> Result<()> {
        self.entity_service(entity_id, "turn_on", service_data)
            .await
    }

    pub async fn turn_off(&self, entity_id: &str) -> Result<()> {
        self.entity_service(entity_id, "turn_off", Value::Null)
            .await
    }

    pub async fn toggle(&self, entity_id: &str) -> Result<()> {
        self.entity_service(entity_id, "toggle", Value::Null).await
    }

    async fn entity_service(&self, entity_id: &str, service: &str, data: Value) -> Result<()> {
        self.call_service(
            domain_of(entity_id),
            service,
            Target::entity(entity_id),
            data,
        )
        .await
        .map(drop)
    }

    /// Subscribes to bus events, optionally of one `event_type` (e.g. `"state_changed"`).
    pub async fn subscribe_events(&self, event_type: Option<&str>) -> Result<Subscription> {
        let mut msg = json!({"type": "subscribe_events"});
        if let Some(event_type) = event_type {
            msg["event_type"] = event_type.into();
        }
        self.subscribe(msg).await
    }

    /// Raw `subscribe_entities` subscription (compressed state diffs), optionally
    /// limited to `entity_ids`. Most callers want [`HaClient::watch_entities`].
    pub async fn subscribe_entities(&self, entity_ids: Option<&[&str]>) -> Result<Subscription> {
        let mut msg = json!({"type": "subscribe_entities"});
        if let Some(ids) = entity_ids {
            msg["entity_ids"] = json!(ids);
        }
        self.subscribe(msg).await
    }

    /// Mirrors entity states locally. The first batch contains every entity as
    /// [`EntityChange::Added`]; later batches carry incremental changes.
    pub async fn watch_entities(&self, entity_ids: Option<&[&str]>) -> Result<EntityWatcher> {
        Ok(EntityWatcher {
            subscription: self.subscribe_entities(entity_ids).await?,
            store: EntityStore::new(),
        })
    }

    fn stamp(&self, msg: Value) -> Result<(u64, Value)> {
        let Value::Object(mut map) = msg else {
            return Err(Error::Protocol("command must be a JSON object".into()));
        };
        let id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
        map.insert("id".into(), id.into());
        Ok((id, Value::Object(map)))
    }

    fn send(&self, msg: &Value) -> Result<()> {
        self.shared
            .out
            .send(Message::text(msg.to_string()))
            .map_err(|_| Error::Disconnected)
    }
}

/// Entities, devices and/or areas a service call acts on.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Target {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub entity_id: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub device_id: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub area_id: Vec<String>,
}

impl Target {
    pub fn entity(entity_id: impl Into<String>) -> Self {
        Self {
            entity_id: vec![entity_id.into()],
            ..Self::default()
        }
    }

    pub fn entities(ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            entity_id: ids.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    pub fn area(area_id: impl Into<String>) -> Self {
        Self {
            area_id: vec![area_id.into()],
            ..Self::default()
        }
    }

    pub fn device(device_id: impl Into<String>) -> Self {
        Self {
            device_id: vec![device_id.into()],
            ..Self::default()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entity_id.is_empty() && self.device_id.is_empty() && self.area_id.is_empty()
    }
}

/// A live subscription. Dropping it unsubscribes.
pub struct Subscription {
    id: u64,
    events: mpsc::UnboundedReceiver<Value>,
    client: HaClient,
    active: bool,
}

impl Subscription {
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The next event payload, or `None` once the connection has closed.
    pub async fn next(&mut self) -> Option<Value> {
        self.events.recv().await
    }

    /// Unsubscribes and waits for Home Assistant to confirm.
    pub async fn unsubscribe(mut self) -> Result<()> {
        self.active = false;
        self.client
            .shared
            .routes
            .lock()
            .unwrap()
            .subscriptions
            .remove(&self.id);
        self.client
            .command(json!({"type": "unsubscribe_events", "subscription": self.id}))
            .await
            .map(drop)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let closed = {
            let mut routes = self.client.shared.routes.lock().unwrap();
            routes.subscriptions.remove(&self.id);
            routes.closed
        };
        if !closed {
            // Fire and forget: the reply has no pending entry and is ignored.
            let msg = json!({"type": "unsubscribe_events", "subscription": self.id});
            if let Ok((_, msg)) = self.client.stamp(msg) {
                let _ = self.client.send(&msg);
            }
        }
    }
}

/// Keeps an [`EntityStore`] in sync and reports each batch of changes.
pub struct EntityWatcher {
    subscription: Subscription,
    store: EntityStore,
}

impl EntityWatcher {
    /// Waits for the next event, applies it, and returns what changed.
    /// Returns `None` once the connection has closed.
    pub async fn next(&mut self) -> Option<Result<Vec<EntityChange>>> {
        let event = self.subscription.next().await?;
        Some(self.store.apply_compressed(&event))
    }

    pub fn store(&self) -> &EntityStore {
        &self.store
    }

    pub fn into_store(self) -> EntityStore {
        self.store
    }
}

async fn run_connection(
    mut ws: WsStream,
    mut out_rx: mpsc::UnboundedReceiver<Message>,
    routes: Arc<Mutex<Routes>>,
) {
    loop {
        tokio::select! {
            frame = ws.next() => match frame {
                Some(Ok(Message::Text(text))) => dispatch(&routes, text.as_str()),
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    tracing::warn!("websocket read failed: {e}");
                    break;
                }
            },
            out = out_rx.recv() => match out {
                Some(msg) => {
                    if let Err(e) = ws.send(msg).await {
                        tracing::warn!("websocket write failed: {e}");
                        break;
                    }
                }
                // Every handle is gone.
                None => {
                    let _ = ws.close(None).await;
                    break;
                }
            },
        }
    }
    // Dropping the senders wakes pending callers with Disconnected and ends subscriptions.
    let mut routes = routes.lock().unwrap();
    routes.closed = true;
    routes.pending.clear();
    routes.subscriptions.clear();
}

fn dispatch(routes: &Mutex<Routes>, text: &str) {
    let messages = match serde_json::from_str::<Value>(text) {
        Ok(Value::Array(messages)) => messages,
        Ok(message) => vec![message],
        Err(e) => {
            tracing::warn!("unparseable message from Home Assistant: {e}");
            return;
        }
    };
    let mut routes = routes.lock().unwrap();
    for message in messages {
        let incoming = match serde_json::from_value::<Incoming>(message) {
            Ok(incoming) => incoming,
            Err(e) => {
                tracing::warn!("unexpected message shape: {e}");
                continue;
            }
        };
        match incoming {
            Incoming::Result {
                id,
                success,
                result,
                error,
            } => {
                if let Some(tx) = routes.pending.remove(&id) {
                    let reply = if success {
                        Ok(result)
                    } else {
                        let error = error.unwrap_or_else(|| crate::protocol::ErrorBody {
                            code: "unknown_error".into(),
                            message: String::new(),
                        });
                        Err(Error::Ha {
                            code: error.code,
                            message: error.message,
                        })
                    };
                    let _ = tx.send(reply);
                }
            }
            Incoming::Pong { id } => {
                if let Some(tx) = routes.pending.remove(&id) {
                    let _ = tx.send(Ok(Value::Null));
                }
            }
            Incoming::Event { id, event } => {
                if let Some(tx) = routes.subscriptions.get(&id)
                    && tx.send(event).is_err()
                {
                    routes.subscriptions.remove(&id);
                }
            }
            other => tracing::debug!("ignoring {other:?}"),
        }
    }
}

async fn next_incoming(ws: &mut WsStream) -> Result<Incoming> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => return Ok(serde_json::from_str(text.as_str())?),
            Some(Ok(Message::Close(_))) | None => return Err(Error::Disconnected),
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(e.into()),
        }
    }
}

/// Normalizes a user-supplied URL to the WebSocket endpoint.
fn websocket_url(input: &str) -> Result<Url> {
    let mut url = Url::parse(input).map_err(|e| Error::InvalidUrl(format!("{input}: {e}")))?;
    let scheme = match url.scheme() {
        "http" | "ws" => "ws",
        "https" | "wss" => "wss",
        other => return Err(Error::InvalidUrl(format!("unsupported scheme `{other}`"))),
    };
    url.set_scheme(scheme)
        .map_err(|()| Error::InvalidUrl(format!("cannot use scheme `{scheme}`")))?;
    if matches!(url.path(), "" | "/") {
        url.set_path("/api/websocket");
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::websocket_url;

    #[test]
    fn normalizes_urls() {
        let cases = [
            (
                "http://homeassistant.local:8123",
                "ws://homeassistant.local:8123/api/websocket",
            ),
            (
                "https://ha.example.com/",
                "wss://ha.example.com/api/websocket",
            ),
            (
                "wss://ha.example.com/api/websocket",
                "wss://ha.example.com/api/websocket",
            ),
            (
                "http://10.0.0.2:8123/custom/ws",
                "ws://10.0.0.2:8123/custom/ws",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(websocket_url(input).unwrap().as_str(), expected, "{input}");
        }
        assert!(websocket_url("ftp://ha").is_err());
        assert!(websocket_url("not a url").is_err());
    }
}
