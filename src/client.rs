//! Connection, authentication, and request/subscription routing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

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

/// Marker pushed to a `subscribe_entities` event channel before the replayed
/// subscription's fresh snapshot, so watchers can drop state from before the
/// reconnect. Home Assistant's compressed events only carry `a`, `c` and `r`
/// keys, so this shape can never arrive from the server.
fn resync_marker() -> Value {
    json!({"resync": true})
}

fn is_resync_marker(event: &Value) -> bool {
    event.get("resync").and_then(Value::as_bool) == Some(true)
}

/// Handle to one authenticated Home Assistant connection.
///
/// Cheap to clone; the connection closes once every clone (and every
/// [`Subscription`]) has been dropped. If the connection drops, pending and
/// future calls fail with [`Error::Disconnected`]; reconnecting is up to the
/// caller unless the client was built with [`HaClient::connect_with_retry`].
#[derive(Clone)]
pub struct HaClient {
    shared: Arc<Shared>,
}

struct Shared {
    next_id: AtomicU64,
    /// Sender for the connection that is currently live; `None` between
    /// connections on a retrying client.
    out: Mutex<Option<mpsc::UnboundedSender<Message>>>,
    routes: Arc<Mutex<Routes>>,
    ha_version: RwLock<String>,
}

#[derive(Default)]
struct Routes {
    closed: bool,
    pending: HashMap<u64, oneshot::Sender<Result<Value>>>,
    subscriptions: HashMap<u64, SubRoute>,
}

/// A live subscription's event channel plus everything needed to re-issue it
/// on a new connection.
struct SubRoute {
    events: mpsc::UnboundedSender<Value>,
    /// The subscribe command as sent (id already stamped), replayed verbatim
    /// after a reconnect.
    msg: Value,
    /// Injected into `events` before a replayed subscription answers, so
    /// stateful consumers can drop stale state.
    on_resync: Option<Value>,
}

/// How [`HaClient::connect_with_retry`] paces reconnect attempts.
///
/// The delay doubles after each failed attempt or short-lived session,
/// starting at `start` and capped at `max`. A session that stayed connected
/// for at least `stable` counts as healthy and resets the delay to `start`,
/// so a permanently broken server is polled at most once per `max` while a
/// blip after hours of uptime reconnects straight away.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Delay before the first reconnect attempt, and after a stable session.
    pub start: Duration,
    /// Upper bound the doubling delay grows toward.
    pub max: Duration,
    /// How long a session must stay connected to reset the delay to `start`.
    pub stable: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            start: Duration::from_secs(2),
            max: Duration::from_secs(60),
            stable: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    fn backoff(&self) -> Backoff {
        let start = self.start.min(self.max);
        Backoff {
            current: start,
            start,
            max: self.max,
        }
    }
}

/// The doubling delay sequence a [`RetryPolicy`] produces.
struct Backoff {
    current: Duration,
    start: Duration,
    max: Duration,
}

impl Backoff {
    fn reset(&mut self) {
        self.current = self.start;
    }
}

impl Iterator for Backoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        let delay = self.current;
        self.current = self.current.saturating_mul(2).min(self.max);
        Some(delay)
    }
}

/// Why a `run_connection` task stopped.
enum ConnEnd {
    /// The socket closed or errored; a supervised client should reconnect.
    Lost,
    /// Every `HaClient`/`Subscription` handle is gone; shut down for good.
    Shutdown,
}

impl HaClient {
    /// Connects and authenticates with a long-lived access token.
    ///
    /// `url` may be the instance's base URL (`http://homeassistant.local:8123`)
    /// or the full WebSocket endpoint (`wss://…/api/websocket`).
    pub async fn connect(url: &str, access_token: &str) -> Result<Self> {
        let url = websocket_url(url)?;
        let (ws, ha_version) = handshake(&url, access_token).await?;
        let client = Self::assemble(ws, ha_version, None);
        client.negotiate_features().await;
        Ok(client)
    }

    /// Like [`HaClient::connect`], but the client reconnects itself for the
    /// rest of its lifetime instead of going dark when the socket drops.
    ///
    /// While the client is between connections, calls keep failing fast with
    /// [`Error::Disconnected`]. After each reconnect, subscriptions created
    /// through this client are re-issued, and [`EntityWatcher`] reports its
    /// previous state as [`EntityChange::Removed`] before yielding the fresh
    /// snapshot, so consumers see a resync rather than silently stale data.
    ///
    /// [`Error::AuthInvalid`] is never retried: a wrong token is returned
    /// immediately here, and on reconnect it stops the client for good.
    pub async fn connect_with_retry(
        url: &str,
        access_token: &str,
        policy: RetryPolicy,
    ) -> Result<Self> {
        let url = websocket_url(url)?;
        let mut backoff = policy.backoff();
        loop {
            match handshake(&url, access_token).await {
                Ok((ws, ha_version)) => {
                    let (done, done_rx) = oneshot::channel();
                    let client = Self::assemble(ws, ha_version, Some(done));
                    tokio::spawn(supervise(
                        Arc::downgrade(&client.shared),
                        url.clone(),
                        access_token.to_owned(),
                        policy,
                        done_rx,
                    ));
                    client.negotiate_features().await;
                    return Ok(client);
                }
                // A bad token can never succeed; everything else may.
                Err(e @ Error::AuthInvalid(_)) => return Err(e),
                Err(e) => {
                    let delay = backoff.next().unwrap_or(policy.max);
                    tracing::warn!("connect failed: {e}; retrying in {delay:?}");
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Builds the shared state and spawns the connection's I/O task.
    /// `done` reports why that task ended; `Some` marks the connection as
    /// supervised, so its subscriptions survive for replay.
    fn assemble(ws: WsStream, ha_version: String, done: Option<oneshot::Sender<ConnEnd>>) -> Self {
        let (out, out_rx) = mpsc::unbounded_channel();
        let routes = Arc::new(Mutex::new(Routes::default()));
        tokio::spawn(run_connection(ws, out_rx, routes.clone(), done));
        HaClient {
            shared: Arc::new(Shared {
                next_id: AtomicU64::new(1),
                out: Mutex::new(Some(out)),
                routes,
                ha_version: RwLock::new(ha_version),
            }),
        }
    }

    /// Lets Home Assistant batch messages into JSON arrays; older versions reject it.
    async fn negotiate_features(&self) {
        let features = json!({"type": "supported_features", "features": {"coalesce_messages": 1}});
        if let Err(e) = self.command(features).await {
            tracing::debug!("message coalescing unavailable: {e}");
        }
    }

    /// The Home Assistant version of the current connection.
    pub fn ha_version(&self) -> String {
        self.shared.ha_version.read().unwrap().clone()
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
        if let Err(e) = self.send(&msg) {
            self.shared.routes.lock().unwrap().pending.remove(&id);
            return Err(e);
        }
        rx.await.map_err(|_| Error::Disconnected)?
    }

    /// Sends a raw subscription command; its events arrive on the returned [`Subscription`].
    pub async fn subscribe(&self, msg: Value) -> Result<Subscription> {
        self.subscribe_route(msg, None).await
    }

    async fn subscribe_route(&self, msg: Value, on_resync: Option<Value>) -> Result<Subscription> {
        let (id, msg) = self.stamp(msg)?;
        let (ack_tx, ack_rx) = oneshot::channel();
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        {
            let mut routes = self.shared.routes.lock().unwrap();
            if routes.closed {
                return Err(Error::Disconnected);
            }
            routes.pending.insert(id, ack_tx);
            routes.subscriptions.insert(
                id,
                SubRoute {
                    events: events_tx,
                    msg: msg.clone(),
                    on_resync,
                },
            );
        }
        if let Err(e) = self.send(&msg) {
            let mut routes = self.shared.routes.lock().unwrap();
            routes.pending.remove(&id);
            routes.subscriptions.remove(&id);
            return Err(e);
        }
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
    ///
    /// On a retrying client the subscription is re-issued after every reconnect;
    /// a `{"resync": true}` event then precedes the fresh snapshot.
    pub async fn subscribe_entities(&self, entity_ids: Option<&[&str]>) -> Result<Subscription> {
        let mut msg = json!({"type": "subscribe_entities"});
        if let Some(ids) = entity_ids {
            msg["entity_ids"] = json!(ids);
        }
        self.subscribe_route(msg, Some(resync_marker())).await
    }

    /// Mirrors entity states locally. The first batch contains every entity as
    /// [`EntityChange::Added`]; later batches carry incremental changes.
    ///
    /// On a retrying client a reconnect produces one batch of
    /// [`EntityChange::Removed`] covering the old state, then a fresh snapshot
    /// of [`EntityChange::Added`]/`Updated` events.
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
        let out = self.shared.out.lock().unwrap();
        let tx = out.as_ref().ok_or(Error::Disconnected)?;
        tx.send(Message::text(msg.to_string()))
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
///
/// On a client built with [`HaClient::connect_with_retry`] a subscription is
/// re-issued after every reconnect and `next` keeps yielding; `None` then only
/// means the client itself is gone or Home Assistant refused the re-issue.
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
    /// On a retrying client this waits through reconnect gaps instead.
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
    ///
    /// After a reconnect on a retrying client, the first batch reports the
    /// pre-reconnect state as [`EntityChange::Removed`] and the following
    /// batch is a fresh snapshot.
    pub async fn next(&mut self) -> Option<Result<Vec<EntityChange>>> {
        loop {
            let event = self.subscription.next().await?;
            if is_resync_marker(&event) {
                let cleared = self.store.clear();
                if cleared.is_empty() {
                    continue;
                }
                return Some(Ok(cleared));
            }
            return Some(self.store.apply_compressed(&event));
        }
    }

    pub fn store(&self) -> &EntityStore {
        &self.store
    }

    pub fn into_store(self) -> EntityStore {
        self.store
    }
}

/// Runs one socket's I/O until it dies or every client handle is gone.
/// `done` reports which; `Some` means a supervisor is listening, so the
/// subscription routes are left standing for the next connection to replay.
async fn run_connection(
    mut ws: WsStream,
    mut out_rx: mpsc::UnboundedReceiver<Message>,
    routes: Arc<Mutex<Routes>>,
    done: Option<oneshot::Sender<ConnEnd>>,
) {
    let mut end = ConnEnd::Lost;
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
                    end = ConnEnd::Shutdown;
                    let _ = ws.close(None).await;
                    break;
                }
            },
        }
    }
    // Dropping the pending senders wakes callers with Disconnected.
    let mut routes = routes.lock().unwrap();
    routes.closed = true;
    routes.pending.clear();
    if done.is_none() {
        // Unsupervised connection: ending subscriptions ends `next()` calls.
        routes.subscriptions.clear();
    }
    drop(routes);
    if let Some(done) = done {
        let _ = done.send(end);
    }
}

/// Reconnect loop behind [`HaClient::connect_with_retry`]. Owns nothing that
/// keeps the client alive: when every `HaClient` and `Subscription` is gone the
/// `Weak` fails (between connections) or the connection task reports
/// [`ConnEnd::Shutdown`], and the loop exits.
async fn supervise(
    weak: Weak<Shared>,
    url: Url,
    access_token: String,
    policy: RetryPolicy,
    mut done: oneshot::Receiver<ConnEnd>,
) {
    let routes = match weak.upgrade() {
        Some(shared) => shared.routes.clone(),
        None => return,
    };
    let mut backoff = policy.backoff();
    let mut connected_at = Instant::now();
    loop {
        match done.await {
            Err(_) | Ok(ConnEnd::Shutdown) => return,
            Ok(ConnEnd::Lost) => {}
        }
        // Only a session that stayed up counts as a success; quick deaths
        // keep doubling so a flapping server is not hammered.
        if connected_at.elapsed() >= policy.stable {
            backoff.reset();
        }
        loop {
            let delay = backoff.next().unwrap_or(policy.max);
            tracing::debug!("reconnecting to Home Assistant in {delay:?}");
            tokio::time::sleep(delay).await;
            let Some(shared) = weak.upgrade() else {
                return;
            };
            match handshake(&url, &access_token).await {
                Ok((ws, ha_version)) => {
                    *shared.ha_version.write().unwrap() = ha_version;
                    let (out, out_rx) = mpsc::unbounded_channel();
                    let (done_tx, done_rx) = oneshot::channel();
                    *shared.out.lock().unwrap() = Some(out);
                    routes.lock().unwrap().closed = false;
                    tokio::spawn(run_connection(ws, out_rx, routes.clone(), Some(done_tx)));
                    let client = HaClient { shared };
                    client.negotiate_features().await;
                    resubscribe(&client).await;
                    tracing::info!("reconnected to Home Assistant");
                    done = done_rx;
                    connected_at = Instant::now();
                    break;
                }
                Err(Error::AuthInvalid(e)) => {
                    tracing::warn!("authentication rejected on reconnect: {e}");
                    return;
                }
                Err(e) => tracing::warn!("reconnect failed: {e}"),
            }
        }
    }
}

/// Re-issues every live subscription on a fresh connection, waiting for each
/// acknowledgement. A refused subscription is dropped, which ends its
/// `Subscription` cleanly instead of leaving it silent.
async fn resubscribe(client: &HaClient) {
    let subs: Vec<(u64, Value, Option<Value>)> = client
        .shared
        .routes
        .lock()
        .unwrap()
        .subscriptions
        .iter()
        .map(|(id, sub)| (*id, sub.msg.clone(), sub.on_resync.clone()))
        .collect();
    let mut acks = Vec::with_capacity(subs.len());
    for (id, msg, on_resync) in subs {
        // Mark the resync before sending, so it always lands ahead of the
        // snapshot the replayed subscription is about to produce.
        if let Some(marker) = on_resync {
            let routes = client.shared.routes.lock().unwrap();
            if let Some(sub) = routes.subscriptions.get(&id) {
                let _ = sub.events.send(marker);
            }
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        client
            .shared
            .routes
            .lock()
            .unwrap()
            .pending
            .insert(id, ack_tx);
        if let Err(e) = client.send(&msg) {
            client.shared.routes.lock().unwrap().pending.remove(&id);
            tracing::debug!("re-subscribe {id} could not be sent: {e}");
            return;
        }
        acks.push((id, ack_rx));
    }
    for (id, ack_rx) in acks {
        match ack_rx.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::warn!("re-subscribe {id} failed: {e}");
                client
                    .shared
                    .routes
                    .lock()
                    .unwrap()
                    .subscriptions
                    .remove(&id);
            }
            // The new connection died mid-replay; the supervisor retries.
            Err(_) => return,
        }
    }
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
                if let Some(sub) = routes.subscriptions.get(&id)
                    && sub.events.send(event).is_err()
                {
                    routes.subscriptions.remove(&id);
                }
            }
            other => tracing::debug!("ignoring {other:?}"),
        }
    }
}

/// Opens the socket and runs the auth handshake; nothing is spawned yet.
async fn handshake(url: &Url, access_token: &str) -> Result<(WsStream, String)> {
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
    match next_incoming(&mut ws).await? {
        Incoming::AuthOk { ha_version } => Ok((ws, ha_version.unwrap_or_default())),
        Incoming::AuthInvalid { message } => Err(Error::AuthInvalid(message.unwrap_or_default())),
        other => Err(Error::Protocol(format!("expected auth_ok, got {other:?}"))),
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
    use super::{RetryPolicy, websocket_url};
    use std::time::Duration;

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

    #[test]
    fn backoff_doubles_to_cap_and_resets() {
        let policy = RetryPolicy {
            start: Duration::from_millis(10),
            max: Duration::from_millis(45),
            stable: Duration::from_secs(30),
        };
        let mut backoff = policy.backoff();
        let delays: Vec<_> = (0..5).map(|_| backoff.next().unwrap()).collect();
        assert_eq!(
            delays,
            [10, 20, 40, 45, 45].map(Duration::from_millis),
            "delays should double to the cap"
        );
        backoff.reset();
        assert_eq!(backoff.next(), Some(Duration::from_millis(10)));
    }

    #[test]
    fn backoff_caps_start_at_max() {
        let policy = RetryPolicy {
            start: Duration::from_secs(90),
            max: Duration::from_secs(60),
            stable: Duration::from_secs(30),
        };
        let mut backoff = policy.backoff();
        assert_eq!(backoff.next(), Some(Duration::from_secs(60)));
        assert_eq!(backoff.next(), Some(Duration::from_secs(60)));
    }
}
