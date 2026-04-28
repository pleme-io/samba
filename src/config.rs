//! Typed config — what the worker reads from `/etc/pleme-worker/config.yaml`.
//!
//! Shape mirrors the `pleme-lib.rate-limit-worker.config` Helm
//! template's output exactly. New fields go in both the chart values
//! and this struct.

use serde::Deserialize;
use std::path::Path;

/// Root config — hydrated by `pleme-lib.rate-limit-worker.config`.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub upstream: UpstreamConfig,
    pub rate_limit: RateLimitConfig,
    pub nats: NatsConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub health: HealthConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamConfig {
    /// Stable kebab-case identifier, e.g. `github`, `datadog`.
    pub kind: String,
    /// Upstream credential's quota per hour.
    pub budget_per_hour: u32,
    /// Headline percentage cap on samba's share of the budget.
    pub budget_pct_max: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RateLimitConfig {
    pub requests_per_minute: u32,
    pub pressure_warn_pct: u8,
    pub pressure_critical_pct: u8,
    pub jitter_pct: f64,
    #[serde(default = "default_burst")]
    pub burst: u32,
    #[serde(default = "default_honor_etag")]
    pub honor_etag: bool,
}

fn default_burst() -> u32 {
    1
}
fn default_honor_etag() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct NatsConfig {
    pub server_url: String,
    pub stream: String,
    pub consumer: String,
    pub result_subject: String,
    pub failed_subject: String,
    #[serde(default = "default_fetch_timeout")]
    pub fetch_timeout: String,
}

fn default_fetch_timeout() -> String {
    "5s".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_port")]
    pub port: u16,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self { port: default_metrics_port() }
    }
}

fn default_metrics_port() -> u16 {
    9090
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealthConfig {
    #[serde(default = "default_health_port")]
    pub port: u16,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self { port: default_health_port() }
    }
}

fn default_health_port() -> u16 {
    8080
}

impl Config {
    /// Load config from a YAML file path.
    ///
    /// # Errors
    /// Returns `Error::Config` for missing file or invalid YAML.
    pub fn load(path: impl AsRef<Path>) -> crate::Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .map_err(|e| crate::Error::Config(format!("read {}: {e}", path.display())))?;
        serde_yaml_ng::from_str(&raw)
            .map_err(|e| crate::Error::Config(format!("parse {}: {e}", path.display())))
    }

    /// Load config from the env var `SAMBA_CONFIG` (default
    /// `/etc/pleme-worker/config.yaml`).
    ///
    /// # Errors
    /// Same as [`Config::load`].
    pub fn from_env() -> crate::Result<Self> {
        let path = std::env::var("SAMBA_CONFIG")
            .unwrap_or_else(|_| "/etc/pleme-worker/config.yaml".to_string());
        Self::load(path)
    }

    /// Validate cross-cutting invariants the schema can't express.
    ///
    /// # Errors
    /// Returns `Error::Config` if any invariant is violated.
    pub fn validate(&self) -> crate::Result<()> {
        if self.rate_limit.pressure_critical_pct > self.rate_limit.pressure_warn_pct {
            return Err(crate::Error::Config(
                "pressure_critical_pct must be <= pressure_warn_pct".into(),
            ));
        }
        // 10% sanity ceiling on the headline budget split.
        let upstream_per_minute = f64::from(self.upstream.budget_per_hour) / 60.0;
        let allowed = upstream_per_minute * (f64::from(self.upstream.budget_pct_max) / 100.0);
        if f64::from(self.rate_limit.requests_per_minute) > allowed * 1.001 {
            return Err(crate::Error::Config(format!(
                "rate_limit.requests_per_minute ({}) exceeds budget_pct_max ({}) of \
                 budget_per_hour ({}) → allowed {:.2}/min",
                self.rate_limit.requests_per_minute,
                self.upstream.budget_pct_max,
                self.upstream.budget_per_hour,
                allowed,
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_budget_share() {
        let mut cfg = base_config();
        cfg.rate_limit.requests_per_minute = 100; // way over 10% of 5000/hr
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn accepts_within_budget() {
        let cfg = base_config();
        assert!(cfg.validate().is_ok());
    }

    fn base_config() -> Config {
        Config {
            upstream: UpstreamConfig {
                kind: "github".into(),
                budget_per_hour: 5000,
                budget_pct_max: 10,
            },
            rate_limit: RateLimitConfig {
                requests_per_minute: 8,
                pressure_warn_pct: 50,
                pressure_critical_pct: 25,
                jitter_pct: 0.30,
                burst: 1,
                honor_etag: true,
            },
            nats: NatsConfig {
                server_url: "nats://localhost:4222".into(),
                stream: "TEST".into(),
                consumer: "test".into(),
                result_subject: "test.results".into(),
                failed_subject: "test.failed".into(),
                fetch_timeout: "5s".into(),
            },
            metrics: MetricsConfig::default(),
            health: HealthConfig::default(),
        }
    }
}
