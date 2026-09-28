use tokio_tungstenite::tungstenite;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid Home Assistant URL: {0}")]
    InvalidUrl(String),
    #[error("websocket error: {0}")]
    WebSocket(#[from] Box<tungstenite::Error>),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("authentication rejected: {0}")]
    AuthInvalid(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    /// Home Assistant answered a command with `success: false`.
    #[error("Home Assistant error `{code}`: {message}")]
    Ha { code: String, message: String },
    #[error("connection to Home Assistant closed")]
    Disconnected,
    /// `connect` exceeded its deadline before the handshake finished.
    #[error("timed out connecting to Home Assistant")]
    Timeout,
}

impl From<tungstenite::Error> for Error {
    fn from(e: tungstenite::Error) -> Self {
        Error::WebSocket(Box::new(e))
    }
}
