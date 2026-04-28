//! `LeakyBucket` — typed pacing primitive.
//!
//! Strict pace enforcement: the bucket admits exactly one token per
//! `60 / requests_per_minute` seconds, with optional ±jitter and
//! adaptive shrinkage when the upstream reports low headroom.
//!
//! The math is the load-bearing rate-limit invariant: under no
//! circumstance can a `LeakyBucket` configured with `rpm = 8` admit
//! more than 8 tokens per minute on average. Burst is configurable
//! but capped (default 1).

use rand::Rng;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Adaptive shrinkage level driven by upstream headroom.
///
/// When the upstream's reported `X-RateLimit-Remaining` drops below
/// configured pressure thresholds (as a percentage of the upstream's
/// `BUDGET_PER_HOUR`), the bucket's effective rate is multiplied by
/// the level's `pace_multiplier()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PressureLevel {
    /// Headroom > `pressure_warn_pct` → full pace.
    Healthy,
    /// `pressure_warn_pct` ≥ headroom > `pressure_critical_pct` → 50%.
    Warn,
    /// `pressure_critical_pct` ≥ headroom > 10% → 25%.
    Critical,
    /// 10% ≥ headroom → 12.5% pace (extreme yielding).
    Emergency,
}

impl PressureLevel {
    /// Multiplier applied to the bucket's nominal admission rate.
    #[must_use]
    pub fn pace_multiplier(self) -> f64 {
        match self {
            Self::Healthy => 1.0,
            Self::Warn => 0.5,
            Self::Critical => 0.25,
            Self::Emergency => 0.125,
        }
    }

    /// Compute the level for an observed `remaining`/`limit` ratio.
    #[must_use]
    pub fn from_headroom(headroom_pct: f64, warn_pct: f64, critical_pct: f64) -> Self {
        if headroom_pct < 10.0 {
            Self::Emergency
        } else if headroom_pct < critical_pct {
            Self::Critical
        } else if headroom_pct < warn_pct {
            Self::Warn
        } else {
            Self::Healthy
        }
    }
}

/// Strict-pace token bucket with adaptive shrinkage and jitter.
///
/// Internally, `acquire().await` blocks until the next token is
/// available. The next-token time is computed from the configured
/// rate × current `PressureLevel` × random jitter. Burst > 1 lets
/// the bucket pre-admit multiple tokens up to the burst ceiling.
#[derive(Debug)]
pub struct LeakyBucket {
    /// Nominal admission rate, requests per minute.
    requests_per_minute: u32,
    /// Headroom % below which the bucket halves.
    pressure_warn_pct: f64,
    /// Headroom % below which the bucket quarters.
    pressure_critical_pct: f64,
    /// ±jitter as a fraction of the nominal interval (0..=1).
    jitter_pct: f64,
    /// Burst capacity (max tokens issued without waiting).
    burst: u32,

    state: Mutex<BucketState>,
}

#[derive(Debug)]
struct BucketState {
    /// Currently held pressure level.
    level: PressureLevel,
    /// Last admission time.
    last_admission: Instant,
    /// Tokens currently held; replenishes at the nominal rate.
    tokens: f64,
}

impl LeakyBucket {
    /// Construct a bucket from typed config. Validates that
    /// `requests_per_minute > 0` and `0.0 <= jitter_pct <= 1.0`.
    ///
    /// # Errors
    /// Returns `Error::Config` for any invalid argument.
    pub fn new(
        requests_per_minute: u32,
        pressure_warn_pct: u8,
        pressure_critical_pct: u8,
        jitter_pct: f64,
        burst: u32,
    ) -> crate::Result<Self> {
        if requests_per_minute == 0 {
            return Err(crate::Error::Config("requests_per_minute must be > 0".into()));
        }
        if !(0.0..=1.0).contains(&jitter_pct) {
            return Err(crate::Error::Config("jitter_pct must be in [0, 1]".into()));
        }
        if burst == 0 {
            return Err(crate::Error::Config("burst must be >= 1".into()));
        }
        if pressure_critical_pct > pressure_warn_pct {
            return Err(crate::Error::Config(
                "pressure_critical_pct must be <= pressure_warn_pct".into(),
            ));
        }

        Ok(Self {
            requests_per_minute,
            pressure_warn_pct: f64::from(pressure_warn_pct),
            pressure_critical_pct: f64::from(pressure_critical_pct),
            jitter_pct,
            burst,
            state: Mutex::new(BucketState {
                level: PressureLevel::Healthy,
                last_admission: Instant::now(),
                tokens: f64::from(burst),
            }),
        })
    }

    /// Block until a token is available, then consume it.
    ///
    /// Returns the wallclock time spent waiting (for metrics).
    pub async fn acquire(&self) -> Duration {
        let start = Instant::now();
        loop {
            let sleep = {
                let mut s = self.state.lock().await;
                self.replenish(&mut s);
                if s.tokens >= 1.0 {
                    s.tokens -= 1.0;
                    s.last_admission = Instant::now();
                    return start.elapsed();
                }
                self.next_admission_wait(&s)
            };
            tokio::time::sleep(sleep).await;
        }
    }

    /// Update the bucket's pressure level based on the observed
    /// upstream headroom. Idempotent — call after every successful
    /// response.
    pub async fn record_headroom(&self, remaining: u32, budget_per_hour: u32) {
        let pct = (f64::from(remaining) / f64::from(budget_per_hour)) * 100.0;
        let new_level = PressureLevel::from_headroom(
            pct,
            self.pressure_warn_pct,
            self.pressure_critical_pct,
        );
        let mut s = self.state.lock().await;
        s.level = new_level;
    }

    /// Current pressure level (cheap read, for metrics).
    pub async fn pressure(&self) -> PressureLevel {
        self.state.lock().await.level
    }

    /// Effective rate (rpm × multiplier, rounded for human display).
    pub async fn effective_rpm(&self) -> f64 {
        let s = self.state.lock().await;
        f64::from(self.requests_per_minute) * s.level.pace_multiplier()
    }

    fn replenish(&self, s: &mut BucketState) {
        let rate_per_sec =
            (f64::from(self.requests_per_minute) * s.level.pace_multiplier()) / 60.0;
        let elapsed = s.last_admission.elapsed().as_secs_f64();
        s.tokens = (s.tokens + elapsed * rate_per_sec).min(f64::from(self.burst));
    }

    fn next_admission_wait(&self, s: &BucketState) -> Duration {
        let rate_per_sec =
            (f64::from(self.requests_per_minute) * s.level.pace_multiplier()) / 60.0;
        // Tokens needed to reach 1.0; compute the wait at current rate.
        let needed = 1.0 - s.tokens;
        let nominal_secs = needed / rate_per_sec;
        // Apply ±jitter%
        let jitter = if self.jitter_pct > 0.0 {
            let mut rng = rand::rng();
            let j: f64 = rng.random_range(-self.jitter_pct..=self.jitter_pct);
            1.0 + j
        } else {
            1.0
        };
        Duration::from_secs_f64((nominal_secs * jitter).max(0.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_levels() {
        // Headroom 100% → Healthy
        assert_eq!(PressureLevel::from_headroom(100.0, 50.0, 25.0), PressureLevel::Healthy);
        // 40% → Warn (below 50% warn threshold)
        assert_eq!(PressureLevel::from_headroom(40.0, 50.0, 25.0), PressureLevel::Warn);
        // 20% → Critical (below 25%)
        assert_eq!(PressureLevel::from_headroom(20.0, 50.0, 25.0), PressureLevel::Critical);
        // 5% → Emergency (below 10%)
        assert_eq!(PressureLevel::from_headroom(5.0, 50.0, 25.0), PressureLevel::Emergency);
    }

    #[test]
    fn pressure_multipliers_decreasing() {
        assert!(PressureLevel::Healthy.pace_multiplier() > PressureLevel::Warn.pace_multiplier());
        assert!(PressureLevel::Warn.pace_multiplier() > PressureLevel::Critical.pace_multiplier());
        assert!(PressureLevel::Critical.pace_multiplier() > PressureLevel::Emergency.pace_multiplier());
    }

    #[test]
    fn rejects_zero_rpm() {
        assert!(LeakyBucket::new(0, 50, 25, 0.3, 1).is_err());
    }

    #[test]
    fn rejects_inverted_pressure_thresholds() {
        // critical > warn is nonsense
        assert!(LeakyBucket::new(8, 25, 50, 0.3, 1).is_err());
    }
}
