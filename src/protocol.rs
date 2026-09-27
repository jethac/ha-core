//! Wire format of messages Home Assistant sends to clients.

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum Incoming {
    AuthRequired {
        #[serde(default)]
        #[allow(dead_code)]
        ha_version: Option<String>,
    },
    AuthOk {
        #[serde(default)]
        ha_version: Option<String>,
    },
    AuthInvalid {
        #[serde(default)]
        message: Option<String>,
    },
    Result {
        id: u64,
        success: bool,
        #[serde(default)]
        result: Value,
        #[serde(default)]
        error: Option<ErrorBody>,
    },
    Event {
        id: u64,
        event: Value,
    },
    Pong {
        id: u64,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ErrorBody {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
}
