//! Typed errors for samba.

use thiserror::Error;

/// Result alias used throughout samba.
pub type Result<T> = std::result::Result<T, Error>;

/// Top-level error type.
#[derive(Debug, Error)]
pub enum Error {
    /// Configuration is invalid (missing field, validation failure).
    #[error("config error: {0}")]
    Config(String),

    /// NATS / JetStream connection or operation failed.
    #[error("nats: {0}")]
    Nats(String),

    /// JSON or YAML serialization failed.
    #[error("serde: {0}")]
    Serde(String),

    /// I/O error (file read, network, etc.).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// Upstream API dispatch failed (the user impl returned an error).
    /// The boxed payload is the upstream's own error type.
    #[error("upstream dispatch failed: {0}")]
    Dispatch(Box<dyn std::error::Error + Send + Sync>),

    /// The metrics HTTP server failed to bind / serve.
    #[error("metrics server: {0}")]
    Metrics(String),
}

impl From<async_nats::Error> for Error {
    fn from(e: async_nats::Error) -> Self {
        Self::Nats(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e.to_string())
    }
}

impl From<serde_yaml_ng::Error> for Error {
    fn from(e: serde_yaml_ng::Error) -> Self {
        Self::Serde(e.to_string())
    }
}
