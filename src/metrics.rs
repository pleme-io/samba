//! Standardized Prometheus metrics for every samba-based worker.
//!
//! The metric names + label set live in this module so every consumer
//! (tend-throttle, datadog-throttle, slack-throttle, …) emits an
//! identical surface — the pleme-lib `_rate_limit_worker.tpl` alert
//! template can therefore be one PromRule shared by all consumers.

use prometheus::{
    register_counter_vec_with_registry, register_gauge_vec_with_registry,
    register_histogram_vec_with_registry, CounterVec, GaugeVec, HistogramVec, Registry,
};
use std::sync::Arc;

use crate::metric_names::{
    BUCKET_TOKENS, DISPATCH_LATENCY, PACE_FACTOR, QUEUE_DEPTH, RATE_LIMIT_REMAINING,
    REQUESTS_TOTAL,
};

/// Collection of all standard metrics. One instance per worker.
#[derive(Clone)]
pub struct Metrics {
    pub registry: Arc<Registry>,
    pub requests_total: CounterVec,
    pub pace_factor: GaugeVec,
    pub rate_limit_remaining: GaugeVec,
    pub queue_depth: GaugeVec,
    pub dispatch_latency: HistogramVec,
    pub bucket_tokens: GaugeVec,
}

impl Metrics {
    /// Build the metrics registry.
    ///
    /// # Errors
    /// Returns `Error::Metrics` if any metric fails to register.
    pub fn new() -> crate::Result<Self> {
        let registry = Arc::new(Registry::new());

        let requests_total = register_counter_vec_with_registry!(
            REQUESTS_TOTAL,
            "Total upstream API requests dispatched",
            &["upstream", "outcome"],
            registry
        )
        .map_err(|e| crate::Error::Metrics(e.to_string()))?;

        let pace_factor = register_gauge_vec_with_registry!(
            PACE_FACTOR,
            "Current adaptive pace multiplier (0..1)",
            &["upstream"],
            registry
        )
        .map_err(|e| crate::Error::Metrics(e.to_string()))?;

        let rate_limit_remaining = register_gauge_vec_with_registry!(
            RATE_LIMIT_REMAINING,
            "Last-seen X-RateLimit-Remaining (or equivalent)",
            &["upstream"],
            registry
        )
        .map_err(|e| crate::Error::Metrics(e.to_string()))?;

        let queue_depth = register_gauge_vec_with_registry!(
            QUEUE_DEPTH,
            "Pending messages in the JetStream consumer (NATS num_pending)",
            &["stream", "consumer"],
            registry
        )
        .map_err(|e| crate::Error::Metrics(e.to_string()))?;

        let dispatch_latency = register_histogram_vec_with_registry!(
            DISPATCH_LATENCY,
            "Dispatch latency in seconds (wall time)",
            &["upstream"],
            registry
        )
        .map_err(|e| crate::Error::Metrics(e.to_string()))?;

        let bucket_tokens = register_gauge_vec_with_registry!(
            BUCKET_TOKENS,
            "Tokens currently in the leaky bucket",
            &["upstream"],
            registry
        )
        .map_err(|e| crate::Error::Metrics(e.to_string()))?;

        Ok(Self {
            registry,
            requests_total,
            pace_factor,
            rate_limit_remaining,
            queue_depth,
            dispatch_latency,
            bucket_tokens,
        })
    }

    /// Outcomes recognized by the standard alert rules. Use these
    /// values for the `outcome` label so PromRule selectors match.
    pub const OUTCOME_SUCCESS: &'static str = "success";
    pub const OUTCOME_CACHED: &'static str = "cached";
    pub const OUTCOME_ERROR: &'static str = "error";
    pub const OUTCOME_TIMEOUT: &'static str = "timeout";
    pub const OUTCOME_RATE_LIMITED: &'static str = "rate_limited";
}
