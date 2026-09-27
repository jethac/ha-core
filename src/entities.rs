//! Entity state, and a local mirror kept current from `subscribe_entities` events.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::Result;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityState {
    pub entity_id: String,
    pub state: String,
    #[serde(default)]
    pub attributes: Map<String, Value>,
    pub last_changed: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

impl EntityState {
    pub fn domain(&self) -> &str {
        domain_of(&self.entity_id)
    }

    /// `friendly_name` attribute, falling back to the entity id.
    pub fn name(&self) -> &str {
        self.attr_str("friendly_name").unwrap_or(&self.entity_id)
    }

    pub fn is_on(&self) -> bool {
        self.state == "on"
    }

    pub fn is_available(&self) -> bool {
        self.state != "unavailable" && self.state != "unknown"
    }

    pub fn attr(&self, key: &str) -> Option<&Value> {
        self.attributes.get(key)
    }

    pub fn attr_str(&self, key: &str) -> Option<&str> {
        self.attr(key)?.as_str()
    }

    pub fn attr_f64(&self, key: &str) -> Option<f64> {
        self.attr(key)?.as_f64()
    }
}

/// The domain part of an entity id: `"light"` for `"light.kitchen"`.
pub fn domain_of(entity_id: &str) -> &str {
    entity_id
        .split_once('.')
        .map_or(entity_id, |(domain, _)| domain)
}

/// What changed in an [`EntityStore`] after applying one event.
#[derive(Debug, Clone, PartialEq)]
pub enum EntityChange {
    Added(EntityState),
    Updated(EntityState),
    Removed(String),
}

/// Local mirror of Home Assistant's entity states.
#[derive(Debug, Clone, Default)]
pub struct EntityStore {
    entities: HashMap<String, EntityState>,
}

impl EntityStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_states(states: impl IntoIterator<Item = EntityState>) -> Self {
        Self {
            entities: states
                .into_iter()
                .map(|s| (s.entity_id.clone(), s))
                .collect(),
        }
    }

    pub fn get(&self, entity_id: &str) -> Option<&EntityState> {
        self.entities.get(entity_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &EntityState> {
        self.entities.values()
    }

    pub fn in_domain<'a>(&'a self, domain: &'a str) -> impl Iterator<Item = &'a EntityState> {
        self.iter().filter(move |s| s.domain() == domain)
    }

    pub fn len(&self) -> usize {
        self.entities.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entities.is_empty()
    }

    /// Applies one `subscribe_entities` event (the compressed format:
    /// `a` = full states, `c` = diffs, `r` = removals) and reports what changed.
    pub fn apply_compressed(&mut self, event: &Value) -> Result<Vec<EntityChange>> {
        let event = CompressedEvent::deserialize(event)?;
        let mut changes = Vec::with_capacity(event.a.len() + event.c.len() + event.r.len());

        for (entity_id, full) in event.a {
            let last_changed = timestamp(full.lc);
            let state = EntityState {
                entity_id: entity_id.clone(),
                state: full.s,
                attributes: full.a,
                last_changed,
                last_updated: full.lu.map_or(last_changed, |lu| timestamp(Some(lu))),
            };
            let change = if self.entities.contains_key(&entity_id) {
                EntityChange::Updated(state.clone())
            } else {
                EntityChange::Added(state.clone())
            };
            self.entities.insert(entity_id, state);
            changes.push(change);
        }

        for entity_id in event.r {
            if self.entities.remove(&entity_id).is_some() {
                changes.push(EntityChange::Removed(entity_id));
            }
        }

        for (entity_id, diff) in event.c {
            let Some(state) = self.entities.get_mut(&entity_id) else {
                tracing::warn!(%entity_id, "diff for unknown entity");
                continue;
            };
            if let Some(add) = diff.add {
                if let Some(s) = add.s {
                    state.state = s;
                }
                if let Some(attrs) = add.a {
                    state.attributes.extend(attrs);
                }
                // A new last_changed implies last_updated moved with it.
                if let Some(lc) = add.lc {
                    state.last_changed = timestamp(Some(lc));
                    state.last_updated = state.last_changed;
                } else if let Some(lu) = add.lu {
                    state.last_updated = timestamp(Some(lu));
                }
            }
            if let Some(remove) = diff.remove {
                for key in remove.a {
                    state.attributes.remove(&key);
                }
            }
            changes.push(EntityChange::Updated(state.clone()));
        }

        Ok(changes)
    }
}

fn timestamp(secs: Option<f64>) -> DateTime<Utc> {
    secs.and_then(|s| DateTime::from_timestamp_micros((s * 1_000_000.0).round() as i64))
        .unwrap_or_default()
}

#[derive(Deserialize)]
struct CompressedEvent {
    #[serde(default)]
    a: HashMap<String, CompressedState>,
    #[serde(default)]
    c: HashMap<String, CompressedDiff>,
    #[serde(default)]
    r: Vec<String>,
}

#[derive(Deserialize)]
struct CompressedState {
    s: String,
    #[serde(default)]
    a: Map<String, Value>,
    #[serde(default)]
    lc: Option<f64>,
    #[serde(default)]
    lu: Option<f64>,
}

#[derive(Deserialize)]
struct CompressedDiff {
    #[serde(rename = "+", default)]
    add: Option<DiffAdd>,
    #[serde(rename = "-", default)]
    remove: Option<DiffRemove>,
}

#[derive(Deserialize)]
struct DiffAdd {
    s: Option<String>,
    a: Option<Map<String, Value>>,
    lc: Option<f64>,
    lu: Option<f64>,
}

#[derive(Deserialize)]
struct DiffRemove {
    #[serde(default)]
    a: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn seeded() -> EntityStore {
        let mut store = EntityStore::new();
        store
            .apply_compressed(&json!({
                "a": {
                    "light.kitchen": {
                        "s": "on",
                        "a": {"friendly_name": "Kitchen", "brightness": 200},
                        "c": "01J0000000000000000000000",
                        "lc": 1_790_000_000.5
                    }
                }
            }))
            .unwrap();
        store
    }

    #[test]
    fn full_state_is_added() {
        let store = seeded();
        let kitchen = store.get("light.kitchen").unwrap();
        assert_eq!(kitchen.name(), "Kitchen");
        assert!(kitchen.is_on());
        assert_eq!(kitchen.attr_f64("brightness"), Some(200.0));
        assert_eq!(kitchen.last_updated, kitchen.last_changed);
        assert_eq!(kitchen.last_changed.timestamp_millis(), 1_790_000_000_500);
    }

    #[test]
    fn diff_updates_state_and_attributes() {
        let mut store = seeded();
        let changes = store
            .apply_compressed(&json!({
                "c": {"light.kitchen": {
                    "+": {"s": "off", "a": {"color_mode": null}, "lc": 1_790_000_010.0},
                    "-": {"a": ["brightness"]}
                }}
            }))
            .unwrap();
        let [EntityChange::Updated(kitchen)] = changes.as_slice() else {
            panic!("unexpected changes: {changes:?}");
        };
        assert_eq!(kitchen.state, "off");
        assert_eq!(kitchen.attr("brightness"), None);
        assert_eq!(kitchen.attr("color_mode"), Some(&Value::Null));
        assert_eq!(kitchen.last_updated.timestamp(), 1_790_000_010);
    }

    #[test]
    fn attribute_only_diff_moves_last_updated_only() {
        let mut store = seeded();
        store
            .apply_compressed(&json!({"c": {"light.kitchen": {"+": {"a": {"brightness": 10}, "lu": 1_790_000_020.0}}}}))
            .unwrap();
        let kitchen = store.get("light.kitchen").unwrap();
        assert_eq!(kitchen.attr_f64("brightness"), Some(10.0));
        assert_eq!(kitchen.last_changed.timestamp(), 1_790_000_000);
        assert_eq!(kitchen.last_updated.timestamp(), 1_790_000_020);
    }

    #[test]
    fn removal_and_unknown_diff() {
        let mut store = seeded();
        let changes = store
            .apply_compressed(
                &json!({"r": ["light.kitchen"], "c": {"switch.ghost": {"+": {"s": "on"}}}}),
            )
            .unwrap();
        assert_eq!(changes, vec![EntityChange::Removed("light.kitchen".into())]);
        assert!(store.is_empty());
    }

    #[test]
    fn get_states_format_deserializes() {
        let state: EntityState = serde_json::from_value(json!({
            "entity_id": "sensor.temp",
            "state": "21.5",
            "attributes": {"unit_of_measurement": "°C"},
            "last_changed": "2026-09-27T10:00:00.123456+00:00",
            "last_updated": "2026-09-27T10:00:00.123456+00:00",
            "context": {"id": "x", "parent_id": null, "user_id": null}
        }))
        .unwrap();
        assert_eq!(state.domain(), "sensor");
        assert_eq!(state.last_changed.timestamp(), 1_790_503_200);
    }
}
