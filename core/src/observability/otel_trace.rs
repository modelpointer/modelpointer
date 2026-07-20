// Copyright 2023-2024 SGLang Team
// Copyright 2026 ModelPointer
//
// SPDX-License-Identifier: Apache-2.0
//
// This file is adapted from sgl-model-gateway/src/observability/otel_trace.rs in the
// SGLang project (https://github.com/sgl-project/sglang).

//! OpenTelemetry tracing integration.

use anyhow::Result;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    propagation::TraceContextPropagator,
    runtime,
    trace::{BatchConfigBuilder, BatchSpanProcessor, Tracer as SdkTracer, TracerProvider},
};
use std::{
    sync::{
        OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::task::spawn_blocking;
use tracing::{Metadata, Subscriber};
use tracing_opentelemetry::{self, OpenTelemetrySpanExt};
use tracing_subscriber::{
    Layer,
    layer::{Context, Filter},
};

/// Target prefixes forwarded to the OpenTelemetry tracing layer.
const ALLOWED_OTEL_TARGET_PREFIXES: &[&str] = &[
    "modelpointer::otel-trace",
    "modelpointer::observability::otel_trace",
];

/// Whether OpenTelemetry tracing is enabled.
///
/// This flag guards access to TRACER and PROVIDER. We use Release/Acquire
/// ordering to ensure proper synchronization: writes to TRACER/PROVIDER
/// happen-before the Release store, and Acquire loads happen-before reads.
static ENABLED: AtomicBool = AtomicBool::new(false);
static TRACER: OnceLock<SdkTracer> = OnceLock::new();
static PROVIDER: OnceLock<TracerProvider> = OnceLock::new();

/// Filter that only allows specific module targets to be exported to OTEL.
#[derive(Clone, Copy, Default)]
pub(crate) struct CustomOtelFilter;

impl CustomOtelFilter {
    #[inline]
    pub const fn new() -> Self {
        Self
    }

    #[inline]
    fn is_allowed(target: &str) -> bool {
        ALLOWED_OTEL_TARGET_PREFIXES
            .iter()
            .any(|prefix| target.starts_with(prefix))
    }
}

impl<S> Filter<S> for CustomOtelFilter
where
    S: Subscriber,
{
    #[inline]
    fn enabled(&self, meta: &Metadata<'_>, _cx: &Context<'_, S>) -> bool {
        Self::is_allowed(meta.target())
    }

    #[inline]
    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> tracing::subscriber::Interest {
        if Self::is_allowed(meta.target()) {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }
}

pub fn otel_tracing_init(
    enable: bool,
    endpoint: &str,
    service_env: &str,
    service_version: &str,
) -> Result<()> {
    if !enable {
        // Use Release to ensure any prior OTEL state changes are visible
        ENABLED.store(false, Ordering::Release);
        return Ok(());
    }

    let endpoint = if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
        format!("http://{endpoint}")
    } else {
        endpoint.to_string()
    };

    global::set_text_map_propagator(TraceContextPropagator::new());

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create HTTP client for OTLP exporter: {e}"))?;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .with_http_client(http_client)
        .with_protocol(Protocol::HttpBinary)
        .build()
        .map_err(|e| {
            eprintln!("[tracing] Failed to create OTLP exporter: {e}");
            anyhow::anyhow!("Failed to create OTLP exporter: {e}")
        })?;

    let batch_config = BatchConfigBuilder::default()
        .with_scheduled_delay(Duration::from_millis(1000))
        .with_max_export_batch_size(512)
        .with_max_queue_size(8192)
        .build();

    let span_processor = BatchSpanProcessor::builder(exporter, runtime::Tokio)
        .with_batch_config(batch_config)
        .build();

    let hostname = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string());

    let resource = Resource::default().merge(&Resource::new(vec![
        KeyValue::new("service.name", "modelpointer"),
        KeyValue::new("service.version", service_version.to_string()),
        KeyValue::new("deployment.environment", service_env.to_string()),
        KeyValue::new("host.name", hostname),
    ]));

    let provider = TracerProvider::builder()
        .with_span_processor(span_processor)
        .with_resource(resource)
        .build();

    PROVIDER
        .set(provider.clone())
        .map_err(|_| anyhow::anyhow!("Provider already initialized"))?;

    let tracer = provider.tracer("modelpointer");

    TRACER
        .set(tracer)
        .map_err(|_| anyhow::anyhow!("Tracer already initialized"))?;

    let _ = global::set_tracer_provider(provider);

    // Use Release ordering: all writes to TRACER/PROVIDER happen-before this store,
    // so any thread that loads ENABLED with Acquire will see the initialized state.
    ENABLED.store(true, Ordering::Release);

    eprintln!("[tracing] OpenTelemetry initialized successfully");
    Ok(())
}

/// Get the OpenTelemetry tracing layer. Must be called after `otel_tracing_init`.
pub fn get_otel_layer<S>() -> Result<Box<dyn Layer<S> + Send + Sync + 'static>>
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
    if !is_otel_enabled() {
        anyhow::bail!("OpenTelemetry is not enabled");
    }

    let tracer = TRACER
        .get()
        .ok_or_else(|| anyhow::anyhow!("Tracer not initialized. Call otel_tracing_init first."))?
        .clone();

    let layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(CustomOtelFilter::new());

    Ok(Box::new(layer))
}

/// Check if OpenTelemetry tracing is enabled.
///
/// Uses Acquire ordering to synchronize with the Release store in `otel_tracing_init`,
/// ensuring that if this returns true, TRACER and PROVIDER are fully initialized.
#[inline]
pub fn is_otel_enabled() -> bool {
    ENABLED.load(Ordering::Acquire)
}

pub async fn flush_spans_async() -> Result<()> {
    if !is_otel_enabled() {
        return Ok(());
    }

    let provider = PROVIDER
        .get()
        .ok_or_else(|| anyhow::anyhow!("Provider not initialized"))?
        .clone();

    spawn_blocking(move || provider.force_flush())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to flush spans: {e}"))?;

    Ok(())
}

pub fn shutdown_otel() {
    // Use Acquire to ensure we see any prior OTEL operations
    if ENABLED.load(Ordering::Acquire) {
        global::shutdown_tracer_provider();
        // Use Release to ensure shutdown completes before flag is cleared
        ENABLED.store(false, Ordering::Release);
        eprintln!("[tracing] OpenTelemetry shut down");
    }
}

/// Inject W3C trace context headers into an HTTP request.
#[inline]
pub fn inject_trace_context_http(headers: &mut HeaderMap) {
    if !is_otel_enabled() {
        return;
    }

    let context = tracing::Span::current().context();

    struct HeaderInjector<'a>(&'a mut HeaderMap);

    impl opentelemetry::propagation::Injector for HeaderInjector<'_> {
        #[inline]
        fn set(&mut self, key: &str, value: String) {
            if let Ok(header_name) = HeaderName::from_bytes(key.as_bytes()) {
                if let Ok(header_value) = HeaderValue::from_str(&value) {
                    self.0.insert(header_name, header_value);
                }
            }
        }
    }

    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut HeaderInjector(headers));
    });
}
