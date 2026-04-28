//! Samba — typed rate-limited consumer primitive for pleme-io.
//!
//! Samba turns the operational pattern *"don't let any caller exceed
//! its share of an upstream API budget"* into a typed Rust primitive.
//! Producers publish to NATS; a single drain (this crate) dispatches
//! at a fixed pace; the rate-limit invariant becomes a theorem of
//! the type system rather than a runtime guess.
//!
//! See the canonical theory in
//! [pleme-io/theory/RATE-LIMITED-CONSUMERS.md][theory].
//!
//! [theory]: https://github.com/pleme-io/theory/blob/main/RATE-LIMITED-CONSUMERS.md
//!
//! # The shape, in one diagram
//!
//! ```text
//! producers ──► JetStream stream ──► [pull consumer w/ MaxAckPending=1]
//!                                                       │
//!                                                       ▼
//!                                            ┌─────────────────────┐
//!                                            │ JetStreamPullWorker │
//!                                            │   ┌─────────────┐   │
//!                                            │   │ LeakyBucket │   │ ← strict pace
//!                                            │   └─────┬───────┘   │
//!                                            │         ▼           │
//!                                            │  ┌─────────────┐    │
//!                                            │  │ UpstreamApi │    │ ← user impl
//!                                            │  └──────┬──────┘    │
//!                                            └─────────┼───────────┘
//!                                                      ▼
//!                                                upstream API
//! ```
//!
//! # Building a consumer in ~30 LOC
//!
//! ```ignore
//! use samba::{Config, JetStreamPullWorker, UpstreamApi};
//!
//! struct GithubApi { client: reqwest::Client, token: String }
//!
//! #[async_trait::async_trait]
//! impl UpstreamApi for GithubApi {
//!     const NAME: &'static str = "github";
//!     const BUDGET_PER_HOUR: u32 = 5_000;
//!     type Request = serde_json::Value;
//!     type Response = serde_json::Value;
//!     type Error = anyhow::Error;
//!
//!     async fn dispatch(&self, _req: Self::Request) -> Result<Self::Response, Self::Error> {
//!         // call api.github.com, return parsed body
//!         todo!()
//!     }
//!
//!     fn rate_limit_remaining(&self, _resp: &Self::Response) -> Option<u32> {
//!         None  // parse X-RateLimit-Remaining
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let cfg = Config::from_env()?;
//!     let upstream = GithubApi { client: reqwest::Client::new(), token: cfg.token().clone() };
//!     JetStreamPullWorker::new(upstream, cfg).run().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Standardized metrics
//!
//! Every samba-based worker emits the same metric names — pleme-lib
//! Helm templates assume this shape:
//!
//! - `samba_requests_total{outcome,upstream}` — counter
//! - `samba_pace_factor{upstream}` — gauge (0..1, current rate multiplier)
//! - `samba_rate_limit_remaining{upstream}` — gauge (last seen X-RateLimit-Remaining)
//! - `samba_queue_depth{stream,consumer}` — gauge
//! - `samba_dispatch_latency_seconds{upstream}` — histogram

#![forbid(unsafe_code)]

pub mod bucket;
pub mod config;
pub mod error;
pub mod metrics;
pub mod upstream;
pub mod worker;

pub use bucket::{LeakyBucket, PressureLevel};
pub use config::{
    Config, NatsConfig, RateLimitConfig, UpstreamConfig, MetricsConfig, HealthConfig,
};
pub use error::{Error, Result};
pub use metrics::Metrics;
pub use upstream::UpstreamApi;
pub use worker::JetStreamPullWorker;

/// Standard metric name constants — every samba-based worker emits
/// these, every pleme-lib Helm template's PrometheusRule references
/// these. Single source of truth.
pub mod metric_names {
    pub const REQUESTS_TOTAL: &str = "samba_requests_total";
    pub const PACE_FACTOR: &str = "samba_pace_factor";
    pub const RATE_LIMIT_REMAINING: &str = "samba_rate_limit_remaining";
    pub const QUEUE_DEPTH: &str = "samba_queue_depth";
    pub const DISPATCH_LATENCY: &str = "samba_dispatch_latency_seconds";
    pub const BUCKET_TOKENS: &str = "samba_bucket_tokens";
}
