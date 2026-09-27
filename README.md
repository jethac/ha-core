# ha-core

Headless Rust client for the [Home Assistant WebSocket API](https://developers.home-assistant.io/docs/api/websocket).
It has no UI dependencies, so a front end (such as a Slint dashboard) can consume it through plain Rust types.

- Connects and authenticates with a long-lived access token. Takes `http(s)://host:8123` or a `ws(s)://…/api/websocket` URL.
- Commands: `get_states`, `get_config`, `get_services`, `call_service`, `turn_on` / `turn_off` / `toggle`, and the area, device and entity registries.
- Live state: `watch_entities` wraps `subscribe_entities`. It keeps an `EntityStore` mirror and yields `Added` / `Updated` / `Removed` batches.
- Raw `command` / `subscribe` calls for anything that isn't wrapped yet. Dropping a subscription unsubscribes it.
- Message coalescing is enabled when the server supports it.

```sh
HA_URL=http://homeassistant.local:8123 HA_TOKEN=... cargo run --example watch
HA_URL=http://homeassistant.local:8123 HA_TOKEN=... cargo run --example toggle light.kitchen
```

Not yet implemented: automatic reconnect (on disconnect, calls fail with `Error::Disconnected` and the caller reconnects) and per-request timeouts (wrap calls in `tokio::time::timeout`).
