//! Headless Home Assistant client over the WebSocket API.
//!
//! `ha-core` has no UI dependencies. It connects and authenticates, then exposes:
//!
//! - one-shot commands ([`HaClient::get_states`], [`HaClient::call_service`], registries),
//! - raw event subscriptions ([`HaClient::subscribe_events`]),
//! - a live entity mirror ([`HaClient::watch_entities`]) built on `subscribe_entities`,
//!   which yields batches of [`EntityChange`]s that a front end can apply to its models,
//! - opt-in reconnect: [`HaClient::connect_with_retry`] re-dials with capped
//!   exponential backoff and re-issues live subscriptions after each drop.
//!   Pass a [`TokenProvider`] instead of a `&str` token to refresh expiring
//!   OAuth access tokens between attempts.
//!
//! ```no_run
//! # async fn demo() -> ha_core::Result<()> {
//! let client = ha_core::HaClient::connect("http://homeassistant.local:8123", "TOKEN").await?;
//! let mut watcher = client.watch_entities(None).await?;
//! while let Some(batch) = watcher.next().await {
//!     for change in batch? {
//!         println!("{change:?}");
//!     }
//! }
//! # Ok(())
//! # }
//! ```

mod client;
mod entities;
mod error;
mod protocol;
mod registry;

pub use client::{
    EntityWatcher, HaClient, HaClientBuilder, RetryPolicy, Subscription, Target, TokenProvider,
};
pub use entities::{EntityChange, EntityState, EntityStore, domain_of};
pub use error::{Error, Result};
pub use registry::{AreaEntry, DeviceEntry, EntityRegistryEntry};
