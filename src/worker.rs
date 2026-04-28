//! `JetStreamPullWorker` — the runtime composition of `UpstreamApi` +
//! `LeakyBucket` + NATS pull consumer + metrics.
//!
//! This is the hot loop every consumer binary runs. New consumers
//! provide an `impl UpstreamApi` and call `JetStreamPullWorker::new(impl, cfg).run().await`.

use crate::{Config, LeakyBucket, Metrics, UpstreamApi};
use async_nats::jetstream;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Pull-based JetStream worker.
///
/// Generic over `UpstreamApi` so each consumer's binary instantiates
/// it with its own typed upstream client.
pub struct JetStreamPullWorker<U: UpstreamApi> {
    upstream: Arc<U>,
    cfg: Config,
    bucket: Arc<LeakyBucket>,
    metrics: Metrics,
}

impl<U: UpstreamApi> JetStreamPullWorker<U> {
    /// Construct from typed upstream + config.
    ///
    /// # Errors
    /// Returns `Error::Config` if the leaky bucket can't be built or
    /// the config validation fails.
    pub fn new(upstream: U, cfg: Config) -> crate::Result<Self> {
        cfg.validate()?;
        let target_rpm = cfg.target_rpm();
        let bucket = Arc::new(LeakyBucket::new(
            target_rpm,
            cfg.rate_limit.pressure_warn_pct,
            cfg.rate_limit.pressure_critical_pct,
            cfg.rate_limit.jitter_pct,
            cfg.rate_limit.burst,
        )?);
        let metrics = Metrics::new()?;
        Ok(Self {
            upstream: Arc::new(upstream),
            cfg,
            bucket,
            metrics,
        })
    }

    /// Run the worker until SIGTERM/SIGINT.
    ///
    /// # Errors
    /// Returns `Error::Nats` for connection failures, `Error::Metrics`
    /// for the HTTP server, and propagates upstream errors only when
    /// fatal (per-message errors are logged + counted, not propagated).
    pub async fn run(self) -> crate::Result<()> {
        info!(
            upstream = U::NAME,
            stream = %self.cfg.nats.stream,
            consumer = %self.cfg.nats.consumer,
            quota_pct = self.cfg.rate_limit.quota_pct,
            target_rpm = self.cfg.target_rpm(),
            target_rph = self.cfg.target_rph(),
            "samba worker starting"
        );

        // Connect to NATS + bind the durable consumer.
        let client = async_nats::connect(&self.cfg.nats.server_url)
            .await
            .map_err(|e| crate::Error::Nats(e.to_string()))?;
        let js = jetstream::new(client.clone());
        let stream = js
            .get_stream(&self.cfg.nats.stream)
            .await
            .map_err(|e| crate::Error::Nats(e.to_string()))?;
        let consumer: jetstream::consumer::PullConsumer = stream
            .get_consumer(&self.cfg.nats.consumer)
            .await
            .map_err(|e| crate::Error::Nats(e.to_string()))?;

        // Spawn the metrics + health HTTP server.
        let metrics_handle = tokio::spawn(serve_http(
            self.cfg.metrics.port,
            self.cfg.health.port,
            self.metrics.clone(),
        ));

        // Main pull loop.
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("shutdown signal received");
                    break;
                }
                result = self.fetch_and_dispatch(&consumer, &client) => {
                    if let Err(e) = result {
                        error!(error = %e, "fetch/dispatch error");
                        // Brief backoff to avoid hot-looping on connection errors.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }

        metrics_handle.abort();
        Ok(())
    }

    async fn fetch_and_dispatch(
        &self,
        consumer: &jetstream::consumer::PullConsumer,
        client: &async_nats::Client,
    ) -> crate::Result<()> {
        use futures::StreamExt;

        let mut messages = consumer
            .fetch()
            .max_messages(1)
            .messages()
            .await
            .map_err(|e| crate::Error::Nats(e.to_string()))?;

        if let Some(msg) = messages.next().await {
            let msg = msg.map_err(|e| crate::Error::Nats(e.to_string()))?;

            // Wait for a token. The leaky bucket enforces the pace
            // ceiling; this is THE load-bearing line.
            let wait = self.bucket.acquire().await;
            debug!(?wait, "token acquired");

            // Update standard pace gauge.
            self.metrics
                .pace_factor
                .with_label_values(&[U::NAME])
                .set(self.bucket.pressure().await.pace_multiplier());

            // Decode + dispatch.
            let payload: U::Request = serde_json::from_slice(&msg.payload)?;
            let timer = self
                .metrics
                .dispatch_latency
                .with_label_values(&[U::NAME])
                .start_timer();
            let outcome = self.upstream.dispatch(payload).await;
            let _elapsed = timer.stop_and_record();

            match outcome {
                Ok(resp) => {
                    let cached = self.upstream.was_cached_response(&resp);
                    let outcome_label = if cached {
                        Metrics::OUTCOME_CACHED
                    } else {
                        Metrics::OUTCOME_SUCCESS
                    };
                    self.metrics
                        .requests_total
                        .with_label_values(&[U::NAME, outcome_label])
                        .inc();

                    // Adaptive shrinkage on observed headroom.
                    if let Some(remaining) = self.upstream.rate_limit_remaining(&resp) {
                        self.bucket
                            .record_headroom(remaining, U::BUDGET_PER_HOUR)
                            .await;
                        self.metrics
                            .rate_limit_remaining
                            .with_label_values(&[U::NAME])
                            .set(f64::from(remaining));
                    }

                    // Publish result + ack.
                    let result_subject = format!(
                        "{}.{}",
                        self.cfg.nats.result_subject,
                        msg.subject.split('.').next_back().unwrap_or("unknown")
                    );
                    let body = serde_json::to_vec(&resp)?;
                    client
                        .publish(result_subject, body.into())
                        .await
                        .map_err(|e| crate::Error::Nats(e.to_string()))?;
                    msg.ack().await.map_err(|e| crate::Error::Nats(e.to_string()))?;
                }
                Err(e) => {
                    warn!(error = %e, "upstream dispatch failed");
                    self.metrics
                        .requests_total
                        .with_label_values(&[U::NAME, Metrics::OUTCOME_ERROR])
                        .inc();
                    // Publish failure + ack-nak so JetStream redelivers
                    // up to maxDeliver. Ack-nak with no payload uses
                    // the default redelivery policy.
                    msg.ack_with(async_nats::jetstream::AckKind::Nak(None))
                        .await
                        .map_err(|e| crate::Error::Nats(e.to_string()))?;
                }
            }
        }
        Ok(())
    }
}

async fn serve_http(metrics_port: u16, health_port: u16, metrics: Metrics) {
    // Two listeners: /metrics on metrics_port (Prometheus scrape) and
    // /healthz + /ready on health_port (kubelet probes). Both are
    // tiny — no router, no middleware. Just enough to satisfy the
    // chart's probe config + ServiceMonitor scrape.
    let m1 = metrics.clone();
    let m2 = metrics;
    tokio::spawn(serve_metrics(metrics_port, m1));
    tokio::spawn(serve_health(health_port, m2));
    futures::future::pending::<()>().await;
}

async fn serve_metrics(port: u16, metrics: Metrics) {
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use hyper_util::rt::{TokioIo, TokioTimer};
    use prometheus::Encoder;

    let addr = format!("0.0.0.0:{port}");
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, %addr, "metrics: bind failed");
            return;
        }
    };
    info!(%addr, "metrics: listening on /metrics");

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(p) => p,
            Err(e) => {
                error!(error = %e, "metrics: accept failed");
                continue;
            }
        };
        let io = TokioIo::new(stream);
        let m = metrics.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req: Request<hyper::body::Incoming>| {
                let m = m.clone();
                async move {
                    if req.method() != Method::GET || req.uri().path() != "/metrics" {
                        return Ok::<_, hyper::Error>(
                            Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body(Full::new(Bytes::from_static(b"not found")))
                                .unwrap(),
                        );
                    }
                    let encoder = prometheus::TextEncoder::new();
                    let mut buf = Vec::new();
                    let _ = encoder.encode(&m.registry.gather(), &mut buf);
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", encoder.format_type())
                        .body(Full::new(Bytes::from(buf)))
                        .unwrap())
                }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .serve_connection(io, svc)
                .await;
        });
    }
}

async fn serve_health(port: u16, _metrics: Metrics) {
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use hyper_util::rt::{TokioIo, TokioTimer};

    let addr = format!("0.0.0.0:{port}");
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, %addr, "health: bind failed");
            return;
        }
    };
    info!(%addr, "health: listening on /healthz, /ready");

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(p) => p,
            Err(e) => {
                error!(error = %e, "health: accept failed");
                continue;
            }
        };
        let io = TokioIo::new(stream);
        tokio::spawn(async move {
            let svc = service_fn(|req: Request<hyper::body::Incoming>| async move {
                let path = req.uri().path();
                let ok = req.method() == Method::GET
                    && (path == "/healthz" || path == "/ready" || path == "/");
                let (status, body): (StatusCode, &'static [u8]) = if ok {
                    (StatusCode::OK, b"ok")
                } else {
                    (StatusCode::NOT_FOUND, b"not found")
                };
                Ok::<_, hyper::Error>(
                    Response::builder()
                        .status(status)
                        .body(Full::new(Bytes::from_static(body)))
                        .unwrap(),
                )
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .serve_connection(io, svc)
                .await;
        });
    }
}
