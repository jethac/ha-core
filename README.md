# ha-core

Headless Rust client for the [Home Assistant WebSocket API](https://developers.home-assistant.io/docs/api/websocket).
It has no UI dependencies, so a front end (such as a Slint dashboard) can consume it through plain Rust types.

- Connects and authenticates with a long-lived access token. Takes `http(s)://host:8123` or a `ws(s)://…/api/websocket` URL. Each connect attempt is bounded by a 30-second deadline (`HaClient::builder().connect_timeout(...)` to change it), so a black-holed host fails with `Error::Timeout` instead of hanging.
- `HaClient::connect_with_retry` reconnects with capped exponential backoff and re-issues live subscriptions after every reconnect; `HaClient::connect` keeps the raw contract where a dropped socket fails calls with `Error::Disconnected`. Pass a `TokenProvider` instead of a `&str` token to refresh expiring OAuth access tokens between attempts.
- Commands: `get_states`, `get_config`, `get_services`, `call_service`, `turn_on` / `turn_off` / `toggle`, and the area, device and entity registries.
- Live state: `watch_entities` wraps `subscribe_entities`. It keeps an `EntityStore` mirror and yields `Added` / `Updated` / `Removed` batches.
- Raw `command` / `subscribe` calls for anything that isn't wrapped yet. Dropping a subscription unsubscribes it.
- Message coalescing is enabled when the server supports it.

```sh
HA_URL=http://homeassistant.local:8123 HA_TOKEN=... cargo run --example watch
HA_URL=http://homeassistant.local:8123 HA_TOKEN=... cargo run --example toggle light.kitchen
```

Not yet implemented: per-request timeouts (wrap calls in `tokio::time::timeout`).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you,
as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
