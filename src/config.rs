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
    /// **The load-bearing knob.** Fraction of `upstream.budget_per_hour`
    /// the consumer is allowed to use, expressed as a decimal in
    /// (0, 1]. Examples:
    ///
    ///   - `0.01` = 1%  → 50 req/hr  (period ≈ 72s)  for GitHub authenticated
    ///   - `0.10` = 10% → 500 req/hr (period ≈ 7.2s) for GitHub authenticated
    ///   - `0.05` = 5%  → 250 req/hr
    ///
    /// `LeakyBucket::new` derives the actual admission interval from
    /// `quota_pct × budget_per_hour / 3600` requests-per-second. No
    /// other knob touches the rate; pressure_*/jitter_/burst all
    /// operate on top of this base.
    ///
    /// Defaults to `0.10` (10% — historical default) so omitting the
    /// field in old configs preserves prior behavior.
    #[serde(default = "default_quota_pct")]
    pub quota_pct: f64,
    /// Optional explicit override of `requests_per_minute`. When set
    /// (>0), takes precedence over `quota_pct`. Useful for testing
    /// or when the upstream's `budget_per_hour` is unknown / unstable.
    /// Generally LEAVE UNSET — let `quota_pct` drive.
    #[serde(default)]
    pub requests_per_minute_override: Option<f64>,
    pub pressure_warn_pct: u8,
    pub pressure_critical_pct: u8,
    pub jitter_pct: f64,
    #[serde(default = "default_burst")]
    pub burst: u32,
    #[serde(default = "default_honor_etag")]
    pub honor_etag: bool,
}

fn default_quota_pct() -> f64 {
    0.10
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

    /// Compute the effective request-per-minute target. If
    /// `requests_per_minute_override` is set, use it; otherwise
    /// derive from `quota_pct × budget_per_hour / 60`.
    #[must_use]
    pub fn target_rpm(&self) -> f64 {
        if let Some(rpm) = self.rate_limit.requests_per_minute_override {
            if rpm > 0.0 {
                return rpm;
            }
        }
        f64::from(self.upstream.budget_per_hour) * self.rate_limit.quota_pct / 60.0
    }

    /// Effective requests-per-hour target (for alert rules + logs).
    #[must_use]
    pub fn target_rph(&self) -> f64 {
        self.target_rpm() * 60.0
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
        if !(0.0..=1.0).contains(&self.rate_limit.quota_pct) {
            return Err(crate::Error::Config(format!(
                "rate_limit.quota_pct ({}) must be in (0, 1]",
                self.rate_limit.quota_pct
            )));
        }
        if self.rate_limit.quota_pct == 0.0
            && self.rate_limit.requests_per_minute_override.is_none()
        {
            return Err(crate::Error::Config(
                "rate_limit.quota_pct=0 with no override means no requests would ever be admitted"
                    .into(),
            ));
        }
        // The headline ceiling: derived rpm must be ≤ budget_pct_max
        // of upstream's per-minute budget. budget_pct_max is the
        // operator-set "absolute cap" (e.g. 10%) that quota_pct must
        // respect.
        let upstream_per_minute = f64::from(self.upstream.budget_per_hour) / 60.0;
        let allowed = upstream_per_minute * (f64::from(self.upstream.budget_pct_max) / 100.0);
        let target = self.target_rpm();
        if target > allowed * 1.001 {
            return Err(crate::Error::Config(format!(
                "target_rpm ({:.2}/min from quota_pct={:.4}) exceeds budget_pct_max ({}%) of \
                 budget_per_hour ({}) → allowed {:.2}/min. Lower quota_pct or raise \
                 budget_pct_max.",
                target,
                self.rate_limit.quota_pct,
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
        // 50% > 10% absolute cap → rejected.
        let mut cfg = base_config();
        cfg.rate_limit.quota_pct = 0.50;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn accepts_within_budget() {
        let cfg = base_config();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn quota_pct_drives_target_rpm() {
        let mut cfg = base_config();
        cfg.rate_limit.quota_pct = 0.01; // 1%
        // 5000/hr × 1% = 50/hr → 50/60 ≈ 0.833 rpm
        let rpm = cfg.target_rpm();
        assert!((rpm - 0.833_333_3).abs() < 0.001, "got {rpm}");
        // budget_pct_max=10 still leaves headroom (0.833 < 8.333) — accepts.
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn override_supersedes_quota_pct() {
        let mut cfg = base_config();
        cfg.rate_limit.quota_pct = 0.10;
        cfg.rate_limit.requests_per_minute_override = Some(2.0);
        assert!((cfg.target_rpm() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn rejects_quota_pct_out_of_range() {
        let mut cfg = base_config();
        cfg.rate_limit.quota_pct = 1.5;
        assert!(cfg.validate().is_err());
        cfg.rate_limit.quota_pct = -0.1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_zero_quota_pct_without_override() {
        let mut cfg = base_config();
        cfg.rate_limit.quota_pct = 0.0;
        cfg.rate_limit.requests_per_minute_override = None;
        assert!(cfg.validate().is_err());
    }

    fn base_config() -> Config {
        Config {
            upstream: UpstreamConfig {
                kind: "github".into(),
                budget_per_hour: 5000,
                budget_pct_max: 10,
            },
            rate_limit: RateLimitConfig {
                quota_pct: 0.10,
                requests_per_minute_override: None,
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
