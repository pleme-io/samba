//! `UpstreamApi` trait — the typed contract every samba-based worker
//! implements. New consumers (Datadog, Slack, GitHub, …) provide one
//! `impl UpstreamApi for FooClient { ... }` block and inherit the
//! whole rate-limited dispatch loop for free.

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

/// One upstream API kind.
///
/// The trait carries enough type information to:
///
/// - dispatch a request and parse the response,
/// - report the upstream's own rate-limit headroom (so the worker can
///   shrink its pace adaptively),
/// - identify itself in metrics + logs by a stable name.
///
/// Implementors are typically thin wrappers over an HTTP client,
/// holding an auth token + base URL.
///
/// # Invariants the trait carries (enforced by `JetStreamPullWorker`)
///
/// 1. The worker NEVER calls `dispatch` faster than the configured
///    pace, regardless of how fast jobs arrive on the queue.
/// 2. `rate_limit_remaining` is consulted on every successful
///    response; the worker uses the result to shrink its bucket size.
/// 3. The worker DOES NOT cache responses — that's the producer's job
///    (they own the ETag / If-None-Match cache).
#[async_trait]
pub trait UpstreamApi: Send + Sync + 'static {
    /// User-facing identifier — appears in `samba_*{upstream=...}`
    /// metric labels, log lines, alert annotations. Use a short
    /// kebab-case slug: `github`, `datadog`, `slack-webhook`, etc.
    const NAME: &'static str;

    /// Authoritative budget for the upstream credential, requests
    /// per hour. The chart's `budgetPerHour` value should match.
    /// Used in alert rules to compute the 10% headroom threshold.
    const BUDGET_PER_HOUR: u32;

    /// The job payload the producer publishes. Must round-trip JSON
    /// because that's the wire format on NATS.
    type Request: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The response payload the worker returns to the producer's
    /// result-subject subscription.
    type Response: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// Implementor's error type. Worker maps to a typed
    /// `samba::Error::Dispatch` variant for redelivery decisions.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Dispatch one request to the upstream API.
    ///
    /// The worker has already taken a token from the leaky bucket
    /// before this is called, so the implementation MUST NOT add its
    /// own pacing or sleep. Just one round-trip; return the response
    /// or the error.
    async fn dispatch(&self, request: Self::Request) -> std::result::Result<Self::Response, Self::Error>;

    /// Report `X-RateLimit-Remaining` (or equivalent) from the
    /// response. `None` if the upstream doesn't expose remaining.
    /// The worker uses this to compute pressure and shrink the
    /// bucket adaptively.
    fn rate_limit_remaining(&self, response: &Self::Response) -> Option<u32>;

    /// Whether a 304-equivalent (cached / no-change) response
    /// occurred — these don't count against the upstream's quota and
    /// shouldn't take a bucket token. Default: never.
    ///
    /// Override for upstreams that support conditional requests
    /// (GitHub `If-None-Match` ETag → 304).
    fn was_cached_response(&self, _response: &Self::Response) -> bool {
        false
    }
}
