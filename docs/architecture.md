# samba — architecture

This document is the in-depth architectural reference for samba. The
short version is in [README.md](../README.md). The fleet-wide pattern
samba is L3 of is in [pleme-io/theory/RATE-LIMITED-CONSUMERS.md][theory].

[theory]: https://github.com/pleme-io/theory/blob/main/RATE-LIMITED-CONSUMERS.md

## The six compounding layers

samba is one of six load-bearing layers in the rate-limited-consumer
pattern. New consumers live entirely at L4 — the lower layers don't
get re-implemented.

```text
L5 Lisp form  (defrate-limited-consumer …)        — aspirational, via #[derive(TataraDomain)]
L4 Consumer   pleme-io/tend's `tend throttle`     — ~30 LOC + 5-line values per upstream
L3 Rust       pleme-io/samba (this crate)         — UpstreamApi + LeakyBucket + JetStreamPullWorker
L2 Broker     pleme-io/helmworks/pleme-nats       — JetStream broker, broker-only
L1 Templates  pleme-io/helmworks/pleme-lib        — _jetstream_stream.tpl, _rate_limit_worker.tpl
L0 Theory     pleme-io/theory                     — RATE-LIMITED-CONSUMERS.md (frame, invariants)
```

samba sits at L3 — the typed Rust primitive every binary stands on.

## Internals

### `UpstreamApi` — typed per-upstream contract

```text
                ┌─────────────────────────────────────────────────┐
                │ trait UpstreamApi                                 │
                ├─────────────────────────────────────────────────┤
                │ const NAME: &str                                  │
                │ const INITIAL_BUDGET_PER_HOUR: u32                │
                │ type Request                                      │
                │ type Response                                     │
                │ type Error                                        │
                │ async fn dispatch(req) -> Result<Response, Error> │
                │ fn rate_limit_remaining(resp) -> Option<u32>      │
                │ fn rate_limit_total(resp) -> Option<u32>          │
                │ fn was_cached_response(resp) -> bool              │
                └─────────────────────────────────────────────────┘
```

One impl per upstream kind. Today: `tend::operator::throttle::TendGithubApi`
(GitHub REST, ~50 LOC including imports). New upstreams = new impl.

### `LeakyBucket` — strict-pace token bucket

```text
                    ┌────────────────────────────┐
                    │       LeakyBucket          │
                    ├────────────────────────────┤
                    │ quota_pct:    f64 (const)  │
                    │ pressure_*:   thresholds   │
                    │ jitter_pct:   ±0..=1       │
                    │ burst:        u32          │
                    │                            │
                    │ Mutex<BucketState>:        │
                    │   requests_per_minute: f64 │
                    │   level: PressureLevel     │
                    │   last_admission: Instant  │
                    │   tokens: f64              │
                    └────────────────────────────┘
```

State machine:

1. `new(quota_pct, initial_rph, ...)` → bucket with
   `rpm = quota_pct × initial_rph / 60`.
2. `acquire().await` → blocks until next token; consumes one.
3. `record_observed_limit(total)` → re-rates: `rpm = quota_pct × total / 60`.
   Called on every response with `X-RateLimit-Limit`.
4. `record_headroom(remaining, total)` → updates `level` based on
   `remaining/total` ratio:
   - `≥ pressure_warn_pct%` → `Healthy` (1.0× pace)
   - `< pressure_warn_pct%` → `Warn` (0.5× pace)
   - `< pressure_critical_pct%` → `Critical` (0.25× pace)
   - `< 10%` → `Emergency` (0.125× pace)

Effective rate at any moment: `rpm × pace_multiplier`.

### `JetStreamPullWorker<U>` — runtime composition

```text
        ┌────────────────────────────────────────────┐
        │ JetStreamPullWorker<U: UpstreamApi>        │
        ├────────────────────────────────────────────┤
        │ upstream:    Arc<U>                        │
        │ cfg:         Config                        │
        │ bucket:      Arc<LeakyBucket>              │
        │ metrics:     Metrics                       │
        └────────────────────────────────────────────┘
```

Hot loop:

```text
loop {
    msg = consumer.fetch().next().await?;          // pull (broker MaxAckPending=1)
    bucket.acquire().await;                         // wait for token
    req = serde_json::from_slice(msg.payload)?;
    timer = metrics.dispatch_latency.start_timer();
    resp = upstream.dispatch(req).await;            // ← user code
    timer.stop();

    if let Some(total) = upstream.rate_limit_total(&resp) {
        bucket.record_observed_limit(total).await;  // ★ dynamic re-rate
    }
    if let Some(rem) = upstream.rate_limit_remaining(&resp) {
        bucket.record_headroom(rem, total).await;   // adaptive shrinkage
    }

    publish(result_subject, resp).await?;
    msg.ack().await?;                               // releases next pull
}
```

The single-token serialization (NATS `MaxAckPending=1`) plus the bucket
acquire is what guarantees no two requests are ever in-flight to the
upstream simultaneously.

## Wire types

```text
┌─────────────────────────────┐         ┌───────────────────────────────┐
│ Producer publishes:          │         │ Worker publishes back:         │
│ <Self::Request>               │         │ <Self::Response>               │
│                               │         │   includes:                    │
│ → tend.github.jobs.refresh    │ ──────▶ │   - rate_limit_remaining       │
│   .<sanitized-key>            │         │   - rate_limit_total           │
│                               │         │   - any user fields            │
│ Subject is per-consumer       │         │ → tend.github.jobs.results     │
│   namespace.                  │         │   .<sanitized-key>             │
└─────────────────────────────┘         └───────────────────────────────┘
```

Standard sanitization (in pleme-io's tend implementation):
`github:owner/repo@HEAD` → `github_owner_repo_HEAD` (replace `:`, `/`,
`@` with `_`).

## Configuration shape

Reflects exactly what `pleme-lib._rate_limit_worker.tpl` renders:

```yaml
upstream:
  kind: "github"
  budget_per_hour: 5000     # cold-start fallback
  budget_pct_max: 10        # absolute ceiling quota_pct can't exceed

rate_limit:
  quota_pct: 0.01           # ★ percentage of upstream's reported limit
  requests_per_minute_override:  # optional escape (testing only)
  pressure_warn_pct: 50
  pressure_critical_pct: 25
  jitter_pct: 0.30
  burst: 1
  honor_etag: true

nats:
  server_url: "nats://pleme-nats.nats.svc:4222"
  stream: "TEND_GITHUB_JOBS"
  consumer: "tend-github-throttle"
  result_subject: "tend.github.jobs.results"
  failed_subject: "tend.github.jobs.failed"
  fetch_timeout: "5s"

metrics: { port: 9090 }
health:  { port: 8080 }
```

## Why the design is what it is

| Decision | Rationale |
|---|---|
| `quota_pct` (not absolute rpm) | Single load-bearing knob. Per-token-type math handled inside samba. |
| `f64` rate (not `u32`) | 1% of 5000/hr = 0.833 rpm, fractional honestly. Integer rounding would be either too aggressive or too conservative. |
| Dynamic limit observation | GitHub reports `X-RateLimit-Limit`; using it means "1%" tracks the credential's actual ceiling without per-deploy reconfiguration. |
| Mutex (not RwLock) on state | Critical section is microseconds; outer waiting loop is for "wait for token" which doesn't need the lock. |
| Trait method (not const) for `rate_limit_total` | Lets implementations expose runtime data; default `None` makes it backward-compat. |
| `MaxAckPending=1` at broker, NOT just bucket | Belt + suspenders. Even if a misbehaving bucket admits, broker refuses to deliver. Two layers, both load-bearing. |
| Subscribe-then-publish in operator's NatsThrottleClient | Avoids race where result fires before subscribe lands. |
| `Nats-Msg-Id: <repo:ref>` on operator publishes | JetStream duplicate_window collapses repeat publishes within 10m; multi-pod is safe. |

## Testing

```bash
cargo test --lib
```

Tests cover (15 total):

- `bucket::tests::pressure_levels` — boundary thresholds
- `bucket::tests::pressure_multipliers_decreasing` — monotonic
- `bucket::tests::accepts_fractional_rpm` — fractional rates work
- `bucket::tests::record_observed_limit_updates_rpm` — dynamic re-rate
- `bucket::tests::record_observed_limit_zero_is_noop` — defensive
- `bucket::tests::rejects_zero_quota_pct`, `_above_one`, `_inverted_pressure_thresholds`, `_zero_initial_rph`
- `config::tests::quota_pct_drives_target_rpm` — math correct
- `config::tests::override_supersedes_quota_pct` — escape hatch
- `config::tests::accepts_within_budget` — 10% under 10% cap OK
- `config::tests::validates_budget_share` — 50% over 10% cap rejected
- `config::tests::rejects_zero_quota_pct_without_override`
- `config::tests::rejects_quota_pct_out_of_range` (negative + > 1)

## Build

```bash
nix flake check        # 12 substrate rust-library checks
cargo build --release  # crates.io-shaped library
```

samba is built via `substrate/lib/rust-library.nix` — same builder
as `hayai`, `shikumi`, etc. (see pleme-io/CLAUDE.md §Substrate flake patterns).
