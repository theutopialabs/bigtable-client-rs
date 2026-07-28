use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

#[cfg(feature = "opentelemetry")]
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};
use tokio::time::Instant;
use tonic::Code;

use crate::ClientConfig;

#[cfg(feature = "opentelemetry")]
const CLIENT_NAME: &str = concat!("bigtable-client-rs/", env!("CARGO_PKG_VERSION"));

/// A high-level Bigtable operation reported by diagnostics.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum BigtableOperation {
    /// A streamed `ReadRows` operation.
    ReadRows,
    /// A bulk operation made from one or more `MutateRows` requests.
    MutateRows,
}

impl BigtableOperation {
    #[must_use]
    pub(crate) const fn method(self) -> &'static str {
        match self {
            Self::ReadRows => "ReadRows",
            Self::MutateRows => "MutateRows",
        }
    }

    #[cfg(feature = "opentelemetry")]
    #[must_use]
    pub(crate) const fn is_streaming(self) -> bool {
        match self {
            Self::ReadRows | Self::MutateRows => true,
        }
    }
}

/// A request lifecycle event sent to a diagnostics observer.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum DiagnosticEvent {
    /// A high-level operation started.
    OperationStarted {
        /// The operation type.
        operation: BigtableOperation,
        /// The validated table ID.
        table_id: String,
        /// Mutation entries submitted by the caller.
        entries: usize,
    },
    /// One RPC attempt started.
    AttemptStarted {
        /// The operation type.
        operation: BigtableOperation,
        /// The zero-based request batch for bulk writes.
        batch: Option<usize>,
        /// The one-based attempt number within the batch or read.
        attempt: u32,
        /// Entries sent by this attempt.
        entries: usize,
        /// Deadline applied to this attempt.
        timeout: Duration,
    },
    /// An RPC attempt received its first response.
    FirstResponse {
        /// The operation type.
        operation: BigtableOperation,
        /// The zero-based request batch for bulk writes.
        batch: Option<usize>,
        /// The one-based attempt number within the batch or read.
        attempt: u32,
        /// Time from attempt start to the first response.
        elapsed: Duration,
    },
    /// A retry was scheduled after a transient failure.
    RetryScheduled {
        /// The operation type.
        operation: BigtableOperation,
        /// The zero-based request batch for bulk writes.
        batch: Option<usize>,
        /// The failed one-based attempt number.
        attempt: u32,
        /// The gRPC status that caused the retry.
        code: Code,
        /// Time to wait before the next attempt.
        delay: Duration,
        /// Entries still unresolved for a bulk write.
        pending_entries: usize,
    },
    /// One RPC attempt finished.
    AttemptFinished {
        /// The operation type.
        operation: BigtableOperation,
        /// The zero-based request batch for bulk writes.
        batch: Option<usize>,
        /// The one-based attempt number within the batch or read.
        attempt: u32,
        /// Final gRPC status for this attempt.
        code: Code,
        /// Total attempt time.
        elapsed: Duration,
        /// Complete rows returned by this attempt.
        rows: u64,
        /// Mutation entries confirmed by this attempt.
        successful_entries: usize,
        /// Mutation entries left unresolved or failed.
        failed_entries: usize,
    },
    /// A high-level operation finished.
    OperationFinished {
        /// The operation type.
        operation: BigtableOperation,
        /// Final operation status.
        code: Code,
        /// Total time across attempts and backoff.
        elapsed: Duration,
        /// RPC attempts made by this operation.
        attempts: u32,
        /// Extra RPC attempts after initial requests.
        retries: u32,
        /// Complete rows returned to the stream.
        rows: u64,
        /// Mutation entries with confirmed success.
        successful_entries: usize,
        /// Mutation entries without confirmed success.
        failed_entries: usize,
    },
}

/// Receives structured diagnostics from high-level client operations.
///
/// The client calls observers inline. Implementations should avoid blocking and
/// must not call back into the same client.
pub trait DiagnosticObserver: Send + Sync + 'static {
    /// Handles one request lifecycle event.
    fn on_event(&self, event: &DiagnosticEvent);
}

impl<F> DiagnosticObserver for F
where
    F: Fn(&DiagnosticEvent) + Send + Sync + 'static,
{
    fn on_event(&self, event: &DiagnosticEvent) {
        self(event);
    }
}

#[derive(Clone)]
pub(crate) struct Telemetry {
    observer: Option<Arc<dyn DiagnosticObserver>>,
    #[cfg(feature = "opentelemetry")]
    metrics: Metrics,
}

impl Telemetry {
    #[cfg(test)]
    pub(crate) fn from_observer(observer: Option<Arc<dyn DiagnosticObserver>>) -> Self {
        #[cfg(feature = "opentelemetry")]
        {
            Self::new(observer, None)
        }
        #[cfg(not(feature = "opentelemetry"))]
        {
            Self::new(observer)
        }
    }

    #[cfg(feature = "opentelemetry")]
    pub(crate) fn new(observer: Option<Arc<dyn DiagnosticObserver>>, meter: Option<Meter>) -> Self {
        let meter = meter.unwrap_or_else(|| opentelemetry::global::meter(env!("CARGO_PKG_NAME")));
        Self {
            observer,
            metrics: Metrics::new(&meter),
        }
    }

    #[cfg(not(feature = "opentelemetry"))]
    pub(crate) const fn new(observer: Option<Arc<dyn DiagnosticObserver>>) -> Self {
        Self { observer }
    }

    pub(crate) fn operation(
        &self,
        config: &ClientConfig,
        operation: BigtableOperation,
        table_id: String,
        entries: usize,
    ) -> OperationTracker {
        let span = tracing::info_span!(
            "bigtable.client.operation",
            otel.name = operation.method(),
            otel.kind = "client",
            otel.status_code = tracing::field::Empty,
            db.system = "gcp.bigtable",
            rpc.system = "grpc",
            rpc.service = "google.bigtable.v2.Bigtable",
            rpc.method = operation.method(),
            gcp.bigtable.project_id = config.project_id(),
            gcp.bigtable.instance_id = config.instance_id(),
            gcp.bigtable.app_profile_id = config.app_profile_id(),
            gcp.bigtable.table_id = table_id.as_str(),
            rpc.grpc.status_code = tracing::field::Empty,
            attempts = tracing::field::Empty,
            retries = tracing::field::Empty,
            rows = tracing::field::Empty,
            successful_entries = tracing::field::Empty,
            failed_entries = tracing::field::Empty,
            elapsed_ms = tracing::field::Empty,
        );
        let state = Arc::new(OperationState {
            telemetry: self.clone(),
            context: MetricContext {
                #[cfg(feature = "opentelemetry")]
                project_id: config.project_id().to_owned(),
                #[cfg(feature = "opentelemetry")]
                instance_id: config.instance_id().to_owned(),
                #[cfg(feature = "opentelemetry")]
                app_profile_id: config.app_profile_id().to_owned(),
                #[cfg(feature = "opentelemetry")]
                table_id: table_id.clone(),
                operation,
            },
            span,
            attempts: AtomicU32::new(0),
            retries: AtomicU32::new(0),
        });
        state.telemetry.emit(&DiagnosticEvent::OperationStarted {
            operation,
            table_id,
            entries,
        });

        OperationTracker {
            state,
            started: Instant::now(),
            finished: false,
        }
    }

    fn emit(&self, event: &DiagnosticEvent) {
        if let Some(observer) = &self.observer {
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer.on_event(event)));
            if result.is_err() {
                tracing::error!("Bigtable diagnostic observer panicked");
            }
        }
    }
}

impl fmt::Debug for Telemetry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Telemetry")
            .field("has_observer", &self.observer.is_some())
            .finish_non_exhaustive()
    }
}

pub(crate) struct OperationTracker {
    state: Arc<OperationState>,
    started: Instant,
    finished: bool,
}

impl OperationTracker {
    pub(crate) fn handle(&self) -> OperationHandle {
        OperationHandle {
            state: Arc::clone(&self.state),
        }
    }

    pub(crate) fn finish(&mut self, code: Code, summary: OperationSummary) {
        if self.finished {
            return;
        }
        self.record_finish(code, summary);
        self.finished = true;
    }

    fn record_finish(&self, code: Code, summary: OperationSummary) {
        let elapsed = self.started.elapsed();
        let attempts = self.state.attempts.load(Ordering::Relaxed);
        let retries = self.state.retries.load(Ordering::Relaxed);
        let code_name = grpc_code_name(code);
        self.state
            .span
            .record("rpc.grpc.status_code", i64::from(i32::from(code)));
        if code != Code::Ok {
            self.state.span.record("otel.status_code", "ERROR");
        }
        self.state.span.record("attempts", attempts);
        self.state.span.record("retries", retries);
        self.state.span.record("rows", summary.rows);
        self.state
            .span
            .record("successful_entries", summary.successful_entries);
        self.state
            .span
            .record("failed_entries", summary.failed_entries);
        self.state
            .span
            .record("elapsed_ms", elapsed.as_secs_f64() * 1_000.0);
        tracing::debug!(
            parent: &self.state.span,
            status = code_name,
            attempts,
            retries,
            rows = summary.rows,
            successful_entries = summary.successful_entries,
            failed_entries = summary.failed_entries,
            elapsed_ms = elapsed.as_secs_f64() * 1_000.0,
            "Bigtable operation finished"
        );
        #[cfg(feature = "opentelemetry")]
        self.state.telemetry.metrics.operation_finished(
            &self.state.context,
            code,
            elapsed,
            retries,
        );
        self.state
            .telemetry
            .emit(&DiagnosticEvent::OperationFinished {
                operation: self.state.context.operation,
                code,
                elapsed,
                attempts,
                retries,
                rows: summary.rows,
                successful_entries: summary.successful_entries,
                failed_entries: summary.failed_entries,
            });
    }
}

impl Drop for OperationTracker {
    fn drop(&mut self) {
        if !self.finished {
            self.record_finish(Code::Cancelled, OperationSummary::default());
        }
    }
}

#[derive(Clone)]
pub(crate) struct OperationHandle {
    state: Arc<OperationState>,
}

impl OperationHandle {
    pub(crate) fn attempt(
        &self,
        batch: Option<usize>,
        attempt: u32,
        entries: usize,
        timeout: Duration,
    ) -> AttemptTracker {
        self.state.attempts.fetch_add(1, Ordering::Relaxed);
        let span = tracing::info_span!(
            parent: &self.state.span,
            "bigtable.client.attempt",
            otel.name = self.state.context.operation.method(),
            otel.kind = "client",
            otel.status_code = tracing::field::Empty,
            rpc.system = "grpc",
            rpc.service = "google.bigtable.v2.Bigtable",
            rpc.method = self.state.context.operation.method(),
            batch = batch.map_or(-1, |value| i64::try_from(value).unwrap_or(i64::MAX)),
            attempt,
            entries,
            timeout_ms = timeout.as_secs_f64() * 1_000.0,
            rpc.grpc.status_code = tracing::field::Empty,
            rows = tracing::field::Empty,
            successful_entries = tracing::field::Empty,
            failed_entries = tracing::field::Empty,
            elapsed_ms = tracing::field::Empty,
        );
        self.state.telemetry.emit(&DiagnosticEvent::AttemptStarted {
            operation: self.state.context.operation,
            batch,
            attempt,
            entries,
            timeout,
        });

        AttemptTracker {
            operation: self.clone(),
            span,
            batch,
            attempt,
            started: Instant::now(),
            first_response: None,
            finished: false,
        }
    }

    pub(crate) fn retry(
        &self,
        batch: Option<usize>,
        attempt: u32,
        code: Code,
        delay: Duration,
        pending_entries: usize,
    ) {
        self.state.retries.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            parent: &self.state.span,
            event = "retry.scheduled",
            batch = batch.map_or(-1, |value| i64::try_from(value).unwrap_or(i64::MAX)),
            attempt,
            code = grpc_code_name(code),
            delay_ms = delay.as_secs_f64() * 1_000.0,
            pending_entries,
            "retrying Bigtable RPC"
        );
        self.state.telemetry.emit(&DiagnosticEvent::RetryScheduled {
            operation: self.state.context.operation,
            batch,
            attempt,
            code,
            delay,
            pending_entries,
        });
    }

    pub(crate) fn application_blocked(&self, elapsed: Duration) {
        #[cfg(feature = "opentelemetry")]
        self.state
            .telemetry
            .metrics
            .application_blocked(&self.state.context, elapsed);

        let _ = self;
        let _ = elapsed;
    }
}

pub(crate) struct AttemptTracker {
    operation: OperationHandle,
    span: tracing::Span,
    batch: Option<usize>,
    attempt: u32,
    started: Instant,
    first_response: Option<Duration>,
    finished: bool,
}

impl AttemptTracker {
    pub(crate) fn span(&self) -> tracing::Span {
        self.span.clone()
    }

    pub(crate) fn first_response(&mut self) {
        if self.first_response.is_some() {
            return;
        }
        let elapsed = self.started.elapsed();
        self.first_response = Some(elapsed);
        self.operation
            .state
            .telemetry
            .emit(&DiagnosticEvent::FirstResponse {
                operation: self.operation.state.context.operation,
                batch: self.batch,
                attempt: self.attempt,
                elapsed,
            });
    }

    pub(crate) fn finish(&mut self, code: Code, summary: AttemptSummary) {
        if self.finished {
            return;
        }
        self.record_finish(code, summary);
        self.finished = true;
    }

    fn record_finish(&self, code: Code, summary: AttemptSummary) {
        let elapsed = self.started.elapsed();
        let code_name = grpc_code_name(code);
        self.span
            .record("rpc.grpc.status_code", i64::from(i32::from(code)));
        if code != Code::Ok {
            self.span.record("otel.status_code", "ERROR");
        }
        self.span.record("rows", summary.rows);
        self.span
            .record("successful_entries", summary.successful_entries);
        self.span.record("failed_entries", summary.failed_entries);
        self.span
            .record("elapsed_ms", elapsed.as_secs_f64() * 1_000.0);
        tracing::debug!(
            parent: &self.span,
            status = code_name,
            rows = summary.rows,
            successful_entries = summary.successful_entries,
            failed_entries = summary.failed_entries,
            elapsed_ms = elapsed.as_secs_f64() * 1_000.0,
            "Bigtable RPC attempt finished"
        );
        #[cfg(feature = "opentelemetry")]
        self.operation.state.telemetry.metrics.attempt_finished(
            &self.operation.state.context,
            code,
            elapsed,
            self.first_response,
        );
        self.operation
            .state
            .telemetry
            .emit(&DiagnosticEvent::AttemptFinished {
                operation: self.operation.state.context.operation,
                batch: self.batch,
                attempt: self.attempt,
                code,
                elapsed,
                rows: summary.rows,
                successful_entries: summary.successful_entries,
                failed_entries: summary.failed_entries,
            });
    }
}

impl Drop for AttemptTracker {
    fn drop(&mut self) {
        if !self.finished {
            self.record_finish(Code::Cancelled, AttemptSummary::default());
        }
    }
}

struct OperationState {
    telemetry: Telemetry,
    context: MetricContext,
    span: tracing::Span,
    attempts: AtomicU32,
    retries: AtomicU32,
}

#[derive(Clone, Debug)]
struct MetricContext {
    #[cfg(feature = "opentelemetry")]
    project_id: String,
    #[cfg(feature = "opentelemetry")]
    instance_id: String,
    #[cfg(feature = "opentelemetry")]
    app_profile_id: String,
    #[cfg(feature = "opentelemetry")]
    table_id: String,
    operation: BigtableOperation,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct OperationSummary {
    pub(crate) rows: u64,
    pub(crate) successful_entries: usize,
    pub(crate) failed_entries: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AttemptSummary {
    pub(crate) rows: u64,
    pub(crate) successful_entries: usize,
    pub(crate) failed_entries: usize,
}

#[cfg(feature = "opentelemetry")]
#[derive(Clone)]
struct Metrics {
    operation_latencies: Histogram<f64>,
    attempt_latencies: Histogram<f64>,
    retry_count: Counter<u64>,
    first_response_latencies: Histogram<f64>,
    application_blocking_latencies: Histogram<f64>,
}

#[cfg(feature = "opentelemetry")]
impl Metrics {
    fn new(meter: &Meter) -> Self {
        Self {
            operation_latencies: meter
                .f64_histogram("bigtable.googleapis.com/client/operation_latencies")
                .with_description("End-to-end Bigtable operation latency across all attempts")
                .with_unit("ms")
                .build(),
            attempt_latencies: meter
                .f64_histogram("bigtable.googleapis.com/client/attempt_latencies")
                .with_description("Client-observed latency for one Bigtable RPC attempt")
                .with_unit("ms")
                .build(),
            retry_count: meter
                .u64_counter("bigtable.googleapis.com/client/retry_count")
                .with_description("Extra Bigtable RPC attempts after initial requests")
                .with_unit("1")
                .build(),
            first_response_latencies: meter
                .f64_histogram("bigtable.googleapis.com/client/first_response_latencies")
                .with_description("Time from attempt start to first response")
                .with_unit("ms")
                .build(),
            application_blocking_latencies: meter
                .f64_histogram("bigtable.googleapis.com/client/application_blocking_latencies")
                .with_description("Time waiting to deliver streamed rows to the application")
                .with_unit("ms")
                .build(),
        }
    }

    fn operation_finished(
        &self,
        context: &MetricContext,
        code: Code,
        elapsed: Duration,
        retries: u32,
    ) {
        let attributes = context.attributes(Some(code));
        self.operation_latencies
            .record(elapsed.as_secs_f64() * 1_000.0, &attributes);
        if retries > 0 {
            self.retry_count.add(u64::from(retries), &attributes);
        }
    }

    fn attempt_finished(
        &self,
        context: &MetricContext,
        code: Code,
        elapsed: Duration,
        first_response: Option<Duration>,
    ) {
        let attributes = context.attributes(Some(code));
        self.attempt_latencies
            .record(elapsed.as_secs_f64() * 1_000.0, &attributes);
        if let Some(first_response) = first_response {
            self.first_response_latencies
                .record(first_response.as_secs_f64() * 1_000.0, &attributes);
        }
    }

    fn application_blocked(&self, context: &MetricContext, elapsed: Duration) {
        self.application_blocking_latencies
            .record(elapsed.as_secs_f64() * 1_000.0, &context.attributes(None));
    }
}

#[cfg(feature = "opentelemetry")]
impl MetricContext {
    fn attributes(&self, code: Option<Code>) -> Vec<KeyValue> {
        let mut attributes = vec![
            KeyValue::new("project_id", self.project_id.clone()),
            KeyValue::new("instance", self.instance_id.clone()),
            KeyValue::new("table", self.table_id.clone()),
            KeyValue::new("app_profile", self.app_profile_id.clone()),
            KeyValue::new("method", self.operation.method()),
            KeyValue::new("streaming", self.operation.is_streaming()),
            KeyValue::new("client_name", CLIENT_NAME),
        ];
        if let Some(code) = code {
            attributes.push(KeyValue::new("status", grpc_code_name(code)));
        }
        attributes
    }
}

pub(crate) const fn grpc_code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

#[cfg(test)]
pub(crate) fn recorded_telemetry() -> (Telemetry, Arc<std::sync::Mutex<Vec<DiagnosticEvent>>>) {
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observer_events = Arc::clone(&events);
    let observer = move |event: &DiagnosticEvent| {
        observer_events
            .lock()
            .expect("diagnostics lock")
            .push(event.clone());
    };
    (Telemetry::from_observer(Some(Arc::new(observer))), events)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    #[cfg(feature = "opentelemetry")]
    use opentelemetry::metrics::MeterProvider as _;
    #[cfg(feature = "opentelemetry")]
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{
            AggregatedMetrics, HistogramDataPoint, MetricData, ResourceMetrics, ScopeMetrics,
            SumDataPoint,
        },
    };
    use tonic::Code;
    use tracing::{
        Id, Subscriber,
        field::{Field, Visit},
        span::{Attributes, Record},
    };
    use tracing_subscriber::{
        Layer, filter::LevelFilter, layer::Context, prelude::*, registry::LookupSpan,
    };

    use super::{
        AttemptSummary, BigtableOperation, DiagnosticEvent, OperationSummary, Telemetry,
        grpc_code_name, recorded_telemetry,
    };
    use crate::ClientConfig;

    fn config() -> ClientConfig {
        ClientConfig::new("project", "instance")
            .expect("valid config")
            .with_app_profile_id("analytics")
            .expect("valid profile")
    }

    #[test]
    fn grpc_codes_use_canonical_metric_names() {
        let cases = [
            (Code::Ok, "OK"),
            (Code::Cancelled, "CANCELLED"),
            (Code::Unknown, "UNKNOWN"),
            (Code::InvalidArgument, "INVALID_ARGUMENT"),
            (Code::DeadlineExceeded, "DEADLINE_EXCEEDED"),
            (Code::NotFound, "NOT_FOUND"),
            (Code::AlreadyExists, "ALREADY_EXISTS"),
            (Code::PermissionDenied, "PERMISSION_DENIED"),
            (Code::ResourceExhausted, "RESOURCE_EXHAUSTED"),
            (Code::FailedPrecondition, "FAILED_PRECONDITION"),
            (Code::Aborted, "ABORTED"),
            (Code::OutOfRange, "OUT_OF_RANGE"),
            (Code::Unimplemented, "UNIMPLEMENTED"),
            (Code::Internal, "INTERNAL"),
            (Code::Unavailable, "UNAVAILABLE"),
            (Code::DataLoss, "DATA_LOSS"),
            (Code::Unauthenticated, "UNAUTHENTICATED"),
        ];

        for (code, expected) in cases {
            assert_eq!(grpc_code_name(code), expected);
        }
    }

    #[test]
    fn diagnostics_keep_operation_attempt_retry_and_summary_order() {
        let (telemetry, events) = recorded_telemetry();
        let mut operation = telemetry.operation(
            &config(),
            BigtableOperation::ReadRows,
            "events".to_owned(),
            0,
        );
        let handle = operation.handle();
        let mut first = handle.attempt(None, 1, 0, Duration::from_secs(5));
        first.first_response();
        first.finish(
            Code::Unavailable,
            AttemptSummary {
                rows: 1,
                ..AttemptSummary::default()
            },
        );
        handle.retry(None, 1, Code::Unavailable, Duration::from_millis(25), 0);
        let mut second = handle.attempt(None, 2, 0, Duration::from_secs(5));
        second.finish(
            Code::Ok,
            AttemptSummary {
                rows: 2,
                ..AttemptSummary::default()
            },
        );
        operation.finish(
            Code::Ok,
            OperationSummary {
                rows: 3,
                ..OperationSummary::default()
            },
        );

        let events = events.lock().expect("diagnostics lock");
        assert_eq!(events.len(), 8);
        assert!(matches!(
            &events[0],
            DiagnosticEvent::OperationStarted {
                operation: BigtableOperation::ReadRows,
                table_id,
                entries: 0,
            } if table_id == "events"
        ));
        assert!(matches!(
            events[1],
            DiagnosticEvent::AttemptStarted {
                attempt: 1,
                batch: None,
                ..
            }
        ));
        assert!(matches!(
            events[2],
            DiagnosticEvent::FirstResponse { attempt: 1, .. }
        ));
        assert!(matches!(
            events[3],
            DiagnosticEvent::AttemptFinished {
                attempt: 1,
                code: Code::Unavailable,
                rows: 1,
                ..
            }
        ));
        assert!(matches!(
            events[4],
            DiagnosticEvent::RetryScheduled {
                attempt: 1,
                code: Code::Unavailable,
                ..
            }
        ));
        assert!(matches!(
            events[5],
            DiagnosticEvent::AttemptStarted { attempt: 2, .. }
        ));
        assert!(matches!(
            events[6],
            DiagnosticEvent::AttemptFinished {
                attempt: 2,
                code: Code::Ok,
                rows: 2,
                ..
            }
        ));
        assert!(matches!(
            events[7],
            DiagnosticEvent::OperationFinished {
                code: Code::Ok,
                attempts: 2,
                retries: 1,
                rows: 3,
                ..
            }
        ));
    }

    #[test]
    fn dropping_trackers_reports_attempt_then_operation_cancellation_once() {
        let (telemetry, events) = recorded_telemetry();
        let operation = telemetry.operation(
            &config(),
            BigtableOperation::MutateRows,
            "events".to_owned(),
            2,
        );
        let attempt = operation
            .handle()
            .attempt(Some(0), 1, 2, Duration::from_secs(5));
        drop(attempt);
        drop(operation);

        let events = events.lock().expect("diagnostics lock");
        assert_eq!(events.len(), 4);
        assert!(matches!(
            events[2],
            DiagnosticEvent::AttemptFinished {
                code: Code::Cancelled,
                ..
            }
        ));
        assert!(matches!(
            events[3],
            DiagnosticEvent::OperationFinished {
                code: Code::Cancelled,
                attempts: 1,
                retries: 0,
                ..
            }
        ));
    }

    #[test]
    fn observer_panics_do_not_stop_request_tracking() {
        let observer = |_event: &DiagnosticEvent| panic!("observer failed");
        let telemetry = Telemetry::from_observer(Some(Arc::new(observer)));
        let mut operation = telemetry.operation(
            &config(),
            BigtableOperation::ReadRows,
            "events".to_owned(),
            0,
        );
        let mut attempt = operation
            .handle()
            .attempt(None, 1, 0, Duration::from_millis(250));

        attempt.finish(Code::Unavailable, AttemptSummary::default());
        operation.finish(Code::Unavailable, OperationSummary::default());
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct SpanRecord {
        name: &'static str,
        parent: Option<&'static str>,
        fields: HashMap<String, String>,
    }

    #[derive(Default)]
    struct FieldVisitor {
        fields: HashMap<String, String>,
    }

    impl Visit for FieldVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.fields
                .insert(field.name().to_owned(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.fields
                .insert(field.name().to_owned(), value.to_owned());
        }
    }

    #[derive(Clone, Default)]
    struct SpanTree {
        spans: Arc<Mutex<Vec<SpanRecord>>>,
    }

    impl<S> Layer<S> for SpanTree
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
            let span = context.span(id).expect("span exists");
            let parent = span.parent().map(|parent| parent.metadata().name());
            let mut fields = FieldVisitor::default();
            attributes.record(&mut fields);
            self.spans.lock().expect("span lock").push(SpanRecord {
                name: span.metadata().name(),
                parent,
                fields: fields.fields,
            });
        }

        fn on_record(&self, id: &Id, values: &Record<'_>, context: Context<'_, S>) {
            let span = context.span(id).expect("span exists");
            let name = span.metadata().name();
            let mut fields = FieldVisitor::default();
            values.record(&mut fields);
            self.spans
                .lock()
                .expect("span lock")
                .iter_mut()
                .find(|record| record.name == name)
                .expect("recorded span")
                .fields
                .extend(fields.fields);
        }
    }

    #[test]
    fn attempt_spans_are_children_of_operation_spans() {
        let layer = SpanTree::default();
        let spans = Arc::clone(&layer.spans);
        let subscriber = tracing_subscriber::registry()
            .with(LevelFilter::TRACE)
            .with(layer);
        let dispatch = tracing::Dispatch::new(subscriber);
        let guard = tracing::dispatcher::set_default(&dispatch);
        tracing::callsite::rebuild_interest_cache();
        let telemetry = Telemetry::from_observer(None);
        let mut operation = telemetry.operation(
            &config(),
            BigtableOperation::ReadRows,
            "events".to_owned(),
            0,
        );
        let mut attempt = operation
            .handle()
            .attempt(None, 1, 0, Duration::from_millis(250));
        attempt.finish(Code::Unavailable, AttemptSummary::default());
        operation.finish(Code::Unavailable, OperationSummary::default());
        drop(guard);
        tracing::callsite::rebuild_interest_cache();

        let spans = spans.lock().expect("span lock");
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].name, "bigtable.client.operation");
        assert_eq!(spans[0].parent, None);
        assert_eq!(spans[0].fields["rpc.method"], "ReadRows");
        assert_eq!(spans[0].fields["gcp.bigtable.table_id"], "events");
        assert_eq!(spans[0].fields["rpc.grpc.status_code"], "14");
        assert_eq!(spans[0].fields["otel.status_code"], "ERROR");
        assert_eq!(spans[1].name, "bigtable.client.attempt");
        assert_eq!(spans[1].parent, Some("bigtable.client.operation"));
        assert_eq!(spans[1].fields["attempt"], "1");
        assert_eq!(spans[1].fields["rpc.grpc.status_code"], "14");
        assert_eq!(spans[1].fields["otel.status_code"], "ERROR");
        assert!(spans.iter().all(|span| {
            span.fields.keys().all(|field| {
                !matches!(
                    field.as_str(),
                    "row_key" | "qualifier" | "value" | "token" | "grpc.message"
                )
            })
        }));
    }

    #[cfg(feature = "opentelemetry")]
    #[test]
    fn metrics_record_google_names_counts_values_and_bounded_attributes() {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("bigtable-client-test");
        let telemetry = Telemetry::new(None, Some(meter));
        let mut operation = telemetry.operation(
            &config(),
            BigtableOperation::MutateRows,
            "events".to_owned(),
            2,
        );
        let handle = operation.handle();
        let mut first = handle.attempt(Some(0), 1, 2, Duration::from_secs(5));
        first.first_response();
        first.finish(
            Code::Unavailable,
            AttemptSummary {
                failed_entries: 2,
                ..AttemptSummary::default()
            },
        );
        handle.retry(Some(0), 1, Code::Unavailable, Duration::from_millis(10), 2);
        let mut second = handle.attempt(Some(0), 2, 2, Duration::from_secs(5));
        second.finish(
            Code::Ok,
            AttemptSummary {
                successful_entries: 2,
                ..AttemptSummary::default()
            },
        );
        handle.application_blocked(Duration::from_millis(3));
        operation.finish(
            Code::Ok,
            OperationSummary {
                successful_entries: 2,
                ..OperationSummary::default()
            },
        );
        provider.force_flush().expect("flush metrics");

        let finished = exporter.get_finished_metrics().expect("finished metrics");
        let metrics = finished
            .iter()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(ScopeMetrics::metrics)
            .map(|metric| (metric.name(), metric))
            .collect::<HashMap<_, _>>();
        assert_eq!(metrics.len(), 5);
        assert_eq!(
            histogram_count(metrics["bigtable.googleapis.com/client/operation_latencies"].data()),
            1
        );
        assert_eq!(
            histogram_count(metrics["bigtable.googleapis.com/client/attempt_latencies"].data()),
            2
        );
        assert_eq!(
            histogram_count(
                metrics["bigtable.googleapis.com/client/first_response_latencies"].data()
            ),
            1
        );
        assert_eq!(
            histogram_count(
                metrics["bigtable.googleapis.com/client/application_blocking_latencies"].data()
            ),
            1
        );
        assert_eq!(
            sum_value(metrics["bigtable.googleapis.com/client/retry_count"].data()),
            1
        );

        let operation_attributes = histogram_attributes(
            metrics["bigtable.googleapis.com/client/operation_latencies"].data(),
        );
        assert_eq!(operation_attributes["project_id"], "project");
        assert_eq!(operation_attributes["instance"], "instance");
        assert_eq!(operation_attributes["table"], "events");
        assert_eq!(operation_attributes["app_profile"], "analytics");
        assert_eq!(operation_attributes["method"], "MutateRows");
        assert_eq!(operation_attributes["streaming"], "true");
        assert_eq!(operation_attributes["status"], "OK");
        assert!(operation_attributes["client_name"].starts_with("bigtable-client-rs/"));
        provider.shutdown().expect("shut down metrics");
    }

    #[cfg(feature = "opentelemetry")]
    fn histogram_count(data: &AggregatedMetrics) -> u64 {
        let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = data else {
            panic!("expected f64 histogram");
        };
        histogram.data_points().map(HistogramDataPoint::count).sum()
    }

    #[cfg(feature = "opentelemetry")]
    fn histogram_attributes(data: &AggregatedMetrics) -> HashMap<String, String> {
        let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = data else {
            panic!("expected f64 histogram");
        };
        histogram
            .data_points()
            .next()
            .expect("histogram point")
            .attributes()
            .map(|attribute| {
                (
                    attribute.key.as_str().to_owned(),
                    attribute.value.to_string(),
                )
            })
            .collect()
    }

    #[cfg(feature = "opentelemetry")]
    fn sum_value(data: &AggregatedMetrics) -> u64 {
        let AggregatedMetrics::U64(MetricData::Sum(sum)) = data else {
            panic!("expected u64 sum");
        };
        sum.data_points().map(SumDataPoint::value).sum()
    }
}
