# samba

> **Typed rate-limited consumer primitive for pleme-io.**
> The Rust crate that turns *"don't let any caller exceed its share of an
> upstream API budget"* from a runtime guess into a theorem of the
> type system.

[![crate](https://img.shields.io/badge/crates.io-samba-orange)](https://crates.io/crates/samba)
[![docs](https://img.shields.io/badge/docs-samba-blue)](https://docs.rs/samba)
[![pleme-io](https://img.shields.io/badge/pleme--io-rate--limit-purple)](https://github.com/pleme-io/theory/blob/main/RATE-LIMITED-CONSUMERS.md)

samba is L3 of the [rate-limited-consumer compounding pattern][theory] —
the typed primitive every fleet-wide rate-limited worker stands on. Its
public surface is three types:

| Type | Role |
|---|---|
| [`UpstreamApi`] (trait) | Typed contract for one upstream API kind. |
| [`LeakyBucket`] | Strict-pace token bucket with adaptive shrinkage + dynamic-rate updates. |
| [`JetStreamPullWorker<U>`] | Hot-loop runtime: NATS pull, bucket admit, dispatch, ack. |

A new rate-limited consumer is `~30 LOC` of `impl UpstreamApi` plus a
chart with a single load-bearing knob: **`quotaPct`** (0–1; e.g. `0.01`
= "use 1% of the upstream's reported quota"). samba does the rest.

[theory]: https://github.com/pleme-io/theory/blob/main/RATE-LIMITED-CONSUMERS.md
[`UpstreamApi`]: src/upstream.rs
[`LeakyBucket`]: src/bucket.rs
[`JetStreamPullWorker<U>`]: src/worker.rs

---

## What it is

```text
producers (operators, daemons, controllers)
       │   publish refresh-job to NATS
       ▼
┌──────────────────────────────────────┐
│ JetStream stream                     │
│ workqueue retention                  │
│ MaxAckPending=1 (broker invariant)   │
└──────────────────────────────────────┘
       │   pulled
       ▼
samba::JetStreamPullWorker<U: UpstreamApi>
       │   admits via LeakyBucket
       │   (rate = quota_pct × upstream's reported limit / 60)
       ▼
   upstream API (e.g. api.github.com)
       │   X-RateLimit-Limit + X-RateLimit-Remaining
       ▼
   bucket adapts: re-rates on observed Limit,
                  shrinks pace_factor on Remaining < threshold
       │
       ▼
   result published, subscribers act
```

## Why it exists

Naive per-caller rate limits (e.g. `tend daemon` running on N
workstations + 1 cluster pod, each capped at 100 req/hr) sum silently
past upstream caps. The fix is **structural**, not procedural:

- Producers can't dispatch HTTP. They publish to NATS.
- Exactly one drain pulls, paced strictly. The drain IS the rate limit.
- `MaxAckPending=1` (broker-side) + `LeakyBucket(quota_pct, observed_limit)`
  (worker-side) = serialized, paced dispatch by construction.

samba is that typed drain. The type system refuses to dispatch faster
than configured, no matter how aggressive the producers.

## Public API

### `UpstreamApi` — typed contract per upstream kind

```rust
#[async_trait::async_trait]
pub trait UpstreamApi: Send + Sync + 'static {
    /// Stable kebab-case identifier for metrics + logs.
    const NAME: &'static str;

    /// Cold-start estimate of the upstream's budget per hour.
    /// FALLBACK ONLY — once a response carries `rate_limit_total`,
    /// samba switches to the observed value.
    const INITIAL_BUDGET_PER_HOUR: u32;

    type Request: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;
    type Response: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Dispatch one request. Bucket has already admitted; just one round-trip.
    async fn dispatch(&self, request: Self::Request)
        -> Result<Self::Response, Self::Error>;

    /// Report `X-RateLimit-Remaining` (or equivalent). Drives PressureLevel.
    fn rate_limit_remaining(&self, response: &Self::Response) -> Option<u32>;

    /// Report `X-RateLimit-Limit` (or equivalent) — the upstream's
    /// CURRENT total budget. samba's bucket re-rates dynamically:
    /// `quota_pct × observed_total / 60` becomes the rpm. Default
    /// `None` means "fall back to INITIAL_BUDGET_PER_HOUR".
    fn rate_limit_total(&self, _response: &Self::Response) -> Option<u32> {
        None
    }

    /// True for 304-equivalent responses that don't count against quota.
    fn was_cached_response(&self, _response: &Self::Response) -> bool {
        false
    }
}
```

### `LeakyBucket` — strict-pace token bucket

```rust
let bucket = LeakyBucket::new(
    quota_pct,         // 0.01 for 1%
    initial_rph,       // fallback before first observation
    pressure_warn_pct, // 50
    pressure_critical_pct, // 25
    jitter_pct,        // 0.30
    burst,             // 1
)?;

bucket.acquire().await; // blocks until next token

// On every response with X-RateLimit-Limit:
bucket.record_observed_limit(observed_total).await;

// On every response with X-RateLimit-Remaining:
bucket.record_headroom(remaining, observed_total).await;
```

### `JetStreamPullWorker<U>` — hot loop

```rust
let cfg = samba::Config::from_env()?;
let upstream = MyGithubClient::new(token);
let worker = JetStreamPullWorker::new(upstream, cfg)?;
worker.run().await?;
```

## Example — GitHub HEAD watcher in 30 LOC

```rust
use samba::{Config, JetStreamPullWorker, UpstreamApi};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct GhRequest { owner: String, repo: String, r#ref: String }

#[derive(Serialize, Deserialize, Debug)]
struct GhResponse {
    sha: String,
    rate_limit_remaining: Option<u32>,
    rate_limit_total: Option<u32>,
}

struct GithubApi { client: reqwest::Client, token: String }

#[async_trait::async_trait]
impl UpstreamApi for GithubApi {
    const NAME: &'static str = "github";
    const INITIAL_BUDGET_PER_HOUR: u32 = 5_000;
    type Request = GhRequest;
    type Response = GhResponse;
    type Error = anyhow::Error;

    async fn dispatch(&self, req: GhRequest) -> anyhow::Result<GhResponse> {
        let url = format!(
            "https://api.github.com/repos/{}/{}/commits/{}",
            req.owner, req.repo, req.r#ref
        );
        let resp = self.client.get(&url)
            .bearer_auth(&self.token)
            .header("User-Agent", "my-throttle")
            .send()
            .await?;
        let remaining = resp.headers().get("x-ratelimit-remaining")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());
        let total = resp.headers().get("x-ratelimit-limit")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());
        let body: serde_json::Value = resp.json().await?;
        Ok(GhResponse {
            sha: body["sha"].as_str().unwrap_or("").to_string(),
            rate_limit_remaining: remaining,
            rate_limit_total: total,
        })
    }

    fn rate_limit_remaining(&self, r: &GhResponse) -> Option<u32> { r.rate_limit_remaining }
    fn rate_limit_total(&self, r: &GhResponse) -> Option<u32> { r.rate_limit_total }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::from_env()?;
    let api = GithubApi {
        client: reqwest::Client::new(),
        token: std::env::var("GITHUB_TOKEN")?,
    };
    JetStreamPullWorker::new(api, cfg)?.run().await?;
    Ok(())
}
```

## Configuration

samba reads YAML from `/etc/pleme-worker/config.yaml` (or path in
`SAMBA_CONFIG`):

```yaml
upstream:
  kind: "github"
  budget_per_hour: 5000        # cold-start fallback only
  budget_pct_max: 10           # absolute cap quota_pct can't exceed

rate_limit:
  quota_pct: 0.01              # ★ THE LOAD-BEARING KNOB (1% here)
  pressure_warn_pct: 50        # halve pace when remaining < 50% of total
  pressure_critical_pct: 25    # quarter pace when remaining < 25%
  jitter_pct: 0.30             # ±30% jitter on inter-request delay
  burst: 1
  honor_etag: true

nats:
  server_url: "nats://nats:4222"
  stream: "TEND_GITHUB_JOBS"
  consumer: "tend-github-throttle"
  result_subject: "tend.github.jobs.results"
  failed_subject: "tend.github.jobs.failed"
  fetch_timeout: "5s"

metrics:
  port: 9090
health:
  port: 8080
```

`quota_pct` is the one knob you tune. samba derives:

```text
target_rpm = quota_pct × observed_X-RateLimit-Limit / 60
```

So `quota_pct: 0.01` against a GitHub fine-grained PAT (5000/hr cap)
gives 0.83 rpm = period ~72s; the same `0.01` against a GitHub App
installation token (15000/hr) gives 2.5 rpm = period ~24s. **No
per-token reconfiguration**.

## Standard metrics

Every samba-based worker emits identical metric names so chart-side
PrometheusRules match across consumers:

| Metric | Type | Labels |
|---|---|---|
| `samba_requests_total` | counter | `outcome={success,cached,error,timeout,rate_limited}, upstream` |
| `samba_pace_factor` | gauge | `upstream` (current 0–1 multiplier) |
| `samba_rate_limit_remaining` | gauge | `upstream` (last seen) |
| `samba_queue_depth` | gauge | `stream, consumer` |
| `samba_dispatch_latency_seconds` | histogram | `upstream` |

## Eight invariants samba guarantees

1. Producers cannot dispatch HTTP — type signature gives them only
   `Publisher`, never `Client`.
2. At most one in-flight request per credential — NATS `MaxAckPending: 1`.
3. Sustained rate ≤ `quota_pct × observed_limit / 60` — `LeakyBucket`
   admits one token per `60 / rpm` seconds.
4. Pressure shrinks rate — `PressureLevel::{Healthy, Warn, Critical, Emergency}`
   gives 1.0/0.5/0.25/0.125 multipliers below configured thresholds.
5. Wallclock-aligned cron is forbidden — `jitter_pct ≥ 0.10` rejected
   at chart-render time.
6. ETag conditional reads don't burn quota — `was_cached_response`
   marks 304s.
7. Every dispatch is observable — `samba_requests_total` mandatory,
   chart refuses to install without ServiceMonitor.
8. Stream + consumer ownership is per-consumer-chart, not central
   broker config — Cilium-style identity.

See [pleme-io/theory/RATE-LIMITED-CONSUMERS.md][theory] §III for the
full invariant frame.

## License

MIT. See [LICENSE](LICENSE).

## Related

- [pleme-io/theory](https://github.com/pleme-io/theory) — the unified
  computing theory; §RATE-LIMITED-CONSUMERS.md is the canonical L0.
- [pleme-io/tend](https://github.com/pleme-io/tend) — the canonical
  consumer (GitHub HEAD watcher / fleet update controller).
- [pleme-io/helmworks](https://github.com/pleme-io/helmworks) —
  charts: `pleme-lib._rate_limit_worker.tpl`, `pleme-nats`,
  `pleme-tend-throttle`.
