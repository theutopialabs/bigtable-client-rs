use std::{pin::Pin, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::{StreamExt, stream};
use googleapis_tonic_google_rpc::google::rpc::Status as RpcStatus;
use prost::Message;
use tokio::time::{Instant, timeout_at};
use tonic::{
    Code, Request, Status,
    metadata::{Ascii, MetadataValue},
};

use crate::{
    BulkMutation, BulkMutationError, BulkMutationPolicyIssue, ClientConfig, Error,
    MutateRowsResponseIssue, MutationFailure, MutationFailureCause, RawClient, RowMutation,
    proto::{MutateRowsRequest, MutateRowsResponse, mutate_rows_request},
    resource::table_name,
    retry::{self, DeadlinePolicy, PolicyIssue, RetryPolicy},
};

const DEFAULT_MAX_ENTRIES_PER_REQUEST: usize = 100;
const DEFAULT_MAX_REQUEST_BYTES: usize = 20 * 1024 * 1024;
const DEFAULT_MAX_IN_FLIGHT_REQUESTS: usize = 5;
const MAX_MUTATIONS_PER_REQUEST: usize = 100_000;

type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<MutateRowsResponse, Status>> + Send + 'static>>;

/// Request splitting and concurrency settings for bulk mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchPolicy {
    /// Maximum row entries targeted for one RPC.
    pub max_entries_per_request: usize,
    /// Maximum encoded bytes targeted for one RPC.
    ///
    /// One larger row entry is still sent by itself.
    pub max_request_bytes: usize,
    /// Maximum concurrent `MutateRows` RPCs.
    pub max_in_flight_requests: usize,
}

impl Default for BatchPolicy {
    fn default() -> Self {
        Self {
            max_entries_per_request: DEFAULT_MAX_ENTRIES_PER_REQUEST,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_in_flight_requests: DEFAULT_MAX_IN_FLIGHT_REQUESTS,
        }
    }
}

/// Retry, deadline, and request splitting settings for bulk mutations.
#[derive(Clone, Debug, PartialEq)]
pub struct BulkMutationOptions {
    /// Retry behavior for unresolved idempotent entries.
    pub retry: RetryPolicy,
    /// Attempt and operation deadlines.
    pub deadlines: DeadlinePolicy,
    /// Request splitting and concurrency limits.
    pub batch: BatchPolicy,
}

impl Default for BulkMutationOptions {
    fn default() -> Self {
        Self {
            retry: RetryPolicy::default(),
            deadlines: DeadlinePolicy {
                operation_timeout: Duration::from_secs(600),
                attempt_timeout: Duration::from_secs(60),
            },
            batch: BatchPolicy::default(),
        }
    }
}

/// Summary of a successful bulk mutation operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BulkMutationResult {
    entries: usize,
    rpc_attempts: u32,
    request_batches: usize,
}

impl BulkMutationResult {
    /// Returns the number of entries with confirmed success.
    #[must_use]
    pub const fn entries(&self) -> usize {
        self.entries
    }

    /// Returns the total RPC attempts, including partial retries.
    #[must_use]
    pub const fn rpc_attempts(&self) -> u32 {
        self.rpc_attempts
    }

    /// Returns the number of request batches before retries.
    #[must_use]
    pub const fn request_batches(&self) -> usize {
        self.request_batches
    }
}

#[async_trait]
trait MutateRowsService: Send + Sync {
    async fn mutate_rows(
        &self,
        request: Request<MutateRowsRequest>,
    ) -> Result<ResponseStream, Status>;
}

struct TonicMutateRowsService {
    raw: RawClient,
}

#[async_trait]
impl MutateRowsService for TonicMutateRowsService {
    async fn mutate_rows(
        &self,
        request: Request<MutateRowsRequest>,
    ) -> Result<ResponseStream, Status> {
        let mut raw = self.raw.clone();
        let response = raw.mutate_rows(request).await?;
        Ok(Box::pin(response.into_inner()))
    }
}

pub(crate) async fn execute(
    raw: RawClient,
    config: &ClientConfig,
    mutation: BulkMutation,
    options: BulkMutationOptions,
) -> Result<BulkMutationResult, Error> {
    execute_with_service(
        Arc::new(TonicMutateRowsService { raw }),
        config,
        mutation,
        options,
    )
    .await
}

async fn execute_with_service(
    service: Arc<dyn MutateRowsService>,
    config: &ClientConfig,
    mutation: BulkMutation,
    options: BulkMutationOptions,
) -> Result<BulkMutationResult, Error> {
    validate_options(&options)?;
    let total_entries = mutation.len();
    if total_entries == 0 {
        return Ok(BulkMutationResult::default());
    }

    let deadline = Instant::now()
        .checked_add(options.deadlines.operation_timeout)
        .ok_or_else(|| {
            Error::invalid_bulk_mutation_policy(BulkMutationPolicyIssue::DeadlineTooLarge)
        })?;
    let (table_id, entries) = mutation.into_parts();
    let table_name = table_name(config, &table_id);
    let routing = routing_header(&table_name)?;
    let batches = split_batches(
        entries,
        &table_name,
        config.app_profile_id(),
        &options.batch,
    );
    let request_batches = batches.len();
    let max_in_flight = options.batch.max_in_flight_requests;
    let app_profile_id = config.app_profile_id().to_owned();
    let outcomes = stream::iter(batches)
        .map(|entries| {
            let service = Arc::clone(&service);
            let options = options.clone();
            let table_name = table_name.clone();
            let app_profile_id = app_profile_id.clone();
            let routing = routing.clone();
            async move {
                BatchOperation {
                    service,
                    table_name,
                    app_profile_id,
                    routing,
                    options,
                    deadline,
                    pending: entries,
                    successful: 0,
                    failures: Vec::new(),
                    rpc_attempts: 0,
                }
                .run()
                .await
            }
        })
        .buffer_unordered(max_in_flight)
        .collect::<Vec<_>>()
        .await;

    let mut successful_entries = 0;
    let mut rpc_attempts = 0_u32;
    let mut failures = Vec::new();
    for outcome in outcomes {
        successful_entries += outcome.successful;
        rpc_attempts = rpc_attempts.saturating_add(outcome.rpc_attempts);
        failures.extend(outcome.failures);
    }
    failures.sort_by_key(MutationFailure::index);

    if failures.is_empty() {
        Ok(BulkMutationResult {
            entries: successful_entries,
            rpc_attempts,
            request_batches,
        })
    } else {
        Err(BulkMutationError {
            total_entries,
            successful_entries,
            rpc_attempts,
            failures,
        }
        .into())
    }
}

fn validate_options(options: &BulkMutationOptions) -> Result<(), Error> {
    retry::validate_policies(&options.retry, &options.deadlines).map_err(|issue| {
        Error::invalid_bulk_mutation_policy(match issue {
            PolicyIssue::ZeroMaxAttempts => BulkMutationPolicyIssue::ZeroMaxAttempts,
            PolicyIssue::ZeroInitialBackoff => BulkMutationPolicyIssue::ZeroInitialBackoff,
            PolicyIssue::ZeroMaxBackoff => BulkMutationPolicyIssue::ZeroMaxBackoff,
            PolicyIssue::MaxBackoffTooSmall => BulkMutationPolicyIssue::MaxBackoffTooSmall,
            PolicyIssue::InvalidBackoffMultiplier => {
                BulkMutationPolicyIssue::InvalidBackoffMultiplier
            }
            PolicyIssue::ZeroOperationTimeout => BulkMutationPolicyIssue::ZeroOperationTimeout,
            PolicyIssue::ZeroAttemptTimeout => BulkMutationPolicyIssue::ZeroAttemptTimeout,
        })
    })?;
    if options.batch.max_entries_per_request == 0 {
        return Err(Error::invalid_bulk_mutation_policy(
            BulkMutationPolicyIssue::ZeroEntriesPerRequest,
        ));
    }
    if options.batch.max_request_bytes == 0 {
        return Err(Error::invalid_bulk_mutation_policy(
            BulkMutationPolicyIssue::ZeroRequestBytes,
        ));
    }
    if options.batch.max_in_flight_requests == 0 {
        return Err(Error::invalid_bulk_mutation_policy(
            BulkMutationPolicyIssue::ZeroInFlightRequests,
        ));
    }
    Ok(())
}

fn routing_header(table_name: &str) -> Result<MetadataValue<Ascii>, Error> {
    format!("table_name={table_name}")
        .parse()
        .map_err(|source| Error::InvalidClientMetadata {
            header: "x-goog-request-params",
            source,
        })
}

#[derive(Clone)]
struct PendingEntry {
    original_index: usize,
    proto: mutate_rows_request::Entry,
    retry_safe: bool,
    attempts: u32,
}

fn split_batches(
    entries: Vec<RowMutation>,
    table_name: &str,
    app_profile_id: &str,
    policy: &BatchPolicy,
) -> Vec<Vec<PendingEntry>> {
    let base_size = MutateRowsRequest {
        table_name: table_name.to_owned(),
        app_profile_id: app_profile_id.to_owned(),
        ..MutateRowsRequest::default()
    }
    .encoded_len();
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = base_size;
    let mut current_mutations = 0;

    for (original_index, row) in entries.into_iter().enumerate() {
        let retry_safe = row.is_retry_safe();
        let proto = row.into_proto();
        let mutation_count = proto.mutations.len();
        let entry_bytes = repeated_message_size(proto.encoded_len());
        let exceeds_target = !current.is_empty()
            && (current.len() == policy.max_entries_per_request
                || current_mutations + mutation_count > MAX_MUTATIONS_PER_REQUEST
                || current_bytes + entry_bytes > policy.max_request_bytes);
        if exceeds_target {
            batches.push(std::mem::take(&mut current));
            current_bytes = base_size;
            current_mutations = 0;
        }
        current_bytes += entry_bytes;
        current_mutations += mutation_count;
        current.push(PendingEntry {
            original_index,
            proto,
            retry_safe,
            attempts: 0,
        });
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

fn repeated_message_size(message_len: usize) -> usize {
    1 + varint_size(message_len) + message_len
}

fn varint_size(mut value: usize) -> usize {
    let mut bytes = 1;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

struct BatchOperation {
    service: Arc<dyn MutateRowsService>,
    table_name: String,
    app_profile_id: String,
    routing: MetadataValue<Ascii>,
    options: BulkMutationOptions,
    deadline: Instant,
    pending: Vec<PendingEntry>,
    successful: usize,
    failures: Vec<MutationFailure>,
    rpc_attempts: u32,
}

struct BatchOutcome {
    successful: usize,
    failures: Vec<MutationFailure>,
    rpc_attempts: u32,
}

impl BatchOperation {
    async fn run(mut self) -> BatchOutcome {
        while !self.pending.is_empty() {
            if Instant::now() >= self.deadline {
                self.fail_pending_deadline();
                break;
            }
            self.rpc_attempts = self.rpc_attempts.saturating_add(1);
            for entry in &mut self.pending {
                entry.attempts = entry.attempts.saturating_add(1);
            }
            let current = std::mem::take(&mut self.pending);
            let attempt_deadline = Instant::now()
                .checked_add(self.options.deadlines.attempt_timeout)
                .map_or(self.deadline, |deadline| deadline.min(self.deadline));
            let request = self.request(&current, attempt_deadline);
            let result = timeout_at(attempt_deadline, self.service.mutate_rows(request)).await;
            let attempt = match result {
                Ok(Ok(stream)) => self.consume_stream(current, stream, attempt_deadline).await,
                Ok(Err(status)) => self.rpc_failure(current, &status),
                Err(_) => self.rpc_failure(
                    current,
                    &Status::deadline_exceeded("MutateRows attempt deadline exceeded"),
                ),
            };
            self.successful += attempt.successful;
            self.failures.extend(attempt.failures);
            self.pending = attempt.retry;

            if self.pending.is_empty() {
                continue;
            }
            let failed_attempt = self
                .pending
                .iter()
                .map(|entry| entry.attempts)
                .max()
                .unwrap_or(1);
            let delay =
                retry::backoff(&self.options.retry, failed_attempt).max(attempt.retry_delay);
            let Some(remaining) = self.deadline.checked_duration_since(Instant::now()) else {
                self.fail_pending_deadline();
                break;
            };
            if delay >= remaining {
                self.fail_pending_deadline();
                break;
            }
            tracing::warn!(
                entries = self.pending.len(),
                attempt = failed_attempt,
                delay_ms = delay.as_millis(),
                "retrying unresolved Bigtable MutateRows entries"
            );
            if timeout_at(self.deadline, tokio::time::sleep(delay))
                .await
                .is_err()
            {
                self.fail_pending_deadline();
                break;
            }
        }

        BatchOutcome {
            successful: self.successful,
            failures: self.failures,
            rpc_attempts: self.rpc_attempts,
        }
    }

    fn request(
        &self,
        entries: &[PendingEntry],
        attempt_deadline: Instant,
    ) -> Request<MutateRowsRequest> {
        let mut request = Request::new(MutateRowsRequest {
            table_name: self.table_name.clone(),
            app_profile_id: self.app_profile_id.clone(),
            entries: entries.iter().map(|entry| entry.proto.clone()).collect(),
            ..MutateRowsRequest::default()
        });
        request
            .metadata_mut()
            .insert("x-goog-request-params", self.routing.clone());
        if let Some(timeout) = attempt_deadline.checked_duration_since(Instant::now()) {
            request.set_timeout(timeout);
        }
        request
    }

    async fn consume_stream(
        &self,
        current: Vec<PendingEntry>,
        mut stream: ResponseStream,
        attempt_deadline: Instant,
    ) -> AttemptOutcome {
        let mut results = (0..current.len()).map(|_| None).collect::<Vec<_>>();
        let mut stream_failure = None;
        let mut protocol_issue = None;

        loop {
            match timeout_at(attempt_deadline, stream.next()).await {
                Ok(Some(Ok(response))) => {
                    for result in response.entries {
                        let Ok(index) = usize::try_from(result.index) else {
                            protocol_issue = Some(MutateRowsResponseIssue::NegativeIndex {
                                index: result.index,
                            });
                            break;
                        };
                        if index >= results.len() {
                            protocol_issue = Some(MutateRowsResponseIssue::IndexOutOfRange {
                                index: result.index,
                                entry_count: results.len(),
                            });
                            break;
                        }
                        if results[index].is_some() {
                            protocol_issue =
                                Some(MutateRowsResponseIssue::DuplicateIndex { index });
                            break;
                        }
                        results[index] = Some(status_result(result.status));
                    }
                    if protocol_issue.is_some() {
                        break;
                    }
                }
                Ok(Some(Err(stream_status))) => {
                    stream_failure = Some(stream_status);
                    break;
                }
                Ok(None) => break,
                Err(_) => {
                    stream_failure = Some(Status::deadline_exceeded(
                        "MutateRows attempt deadline exceeded",
                    ));
                    break;
                }
            }
        }

        if let Some(issue) = protocol_issue {
            return AttemptOutcome {
                successful: 0,
                failures: current
                    .into_iter()
                    .map(|entry| MutationFailure {
                        index: entry.original_index,
                        attempts: entry.attempts,
                        cause: MutationFailureCause::InvalidResponse {
                            issue: issue.clone(),
                        },
                    })
                    .collect(),
                retry: Vec::new(),
                retry_delay: Duration::ZERO,
            };
        }

        let mut outcome = AttemptOutcome::default();
        for (local_index, (entry, result)) in current.into_iter().zip(results).enumerate() {
            match result {
                Some(Ok(())) => outcome.successful += 1,
                Some(Err(entry_status)) => {
                    self.classify_status(entry, entry_status, FailureOrigin::Entry, &mut outcome);
                }
                None => {
                    if let Some(status) = &stream_failure {
                        self.classify_status(
                            entry,
                            status.clone(),
                            FailureOrigin::Rpc,
                            &mut outcome,
                        );
                    } else {
                        outcome.failures.push(MutationFailure {
                            index: entry.original_index,
                            attempts: entry.attempts,
                            cause: MutationFailureCause::InvalidResponse {
                                issue: MutateRowsResponseIssue::MissingIndex { index: local_index },
                            },
                        });
                    }
                }
            }
        }
        outcome
    }

    fn rpc_failure(&self, current: Vec<PendingEntry>, status: &Status) -> AttemptOutcome {
        let mut outcome = AttemptOutcome::default();
        for entry in current {
            self.classify_status(entry, status.clone(), FailureOrigin::Rpc, &mut outcome);
        }
        outcome
    }

    fn classify_status(
        &self,
        entry: PendingEntry,
        status: Status,
        origin: FailureOrigin,
        outcome: &mut AttemptOutcome,
    ) {
        let can_retry = entry.retry_safe
            && retry::is_mutate_retryable(&status)
            && entry.attempts < self.options.retry.max_attempts;
        if can_retry {
            outcome.retry_delay = outcome
                .retry_delay
                .max(retry::retry_delay(&status).unwrap_or(Duration::ZERO));
            outcome.retry.push(entry);
            return;
        }

        let retry_safe = entry.retry_safe;
        let cause = match origin {
            FailureOrigin::Entry => MutationFailureCause::EntryStatus { status, retry_safe },
            FailureOrigin::Rpc => MutationFailureCause::RpcStatus { status, retry_safe },
        };
        outcome.failures.push(MutationFailure {
            index: entry.original_index,
            attempts: entry.attempts,
            cause,
        });
    }

    fn fail_pending_deadline(&mut self) {
        self.failures
            .extend(self.pending.drain(..).map(|entry| MutationFailure {
                index: entry.original_index,
                attempts: entry.attempts,
                cause: MutationFailureCause::DeadlineExceeded {
                    timeout: self.options.deadlines.operation_timeout,
                },
            }));
    }
}

#[derive(Default)]
struct AttemptOutcome {
    successful: usize,
    failures: Vec<MutationFailure>,
    retry: Vec<PendingEntry>,
    retry_delay: Duration,
}

#[derive(Clone, Copy)]
enum FailureOrigin {
    Entry,
    Rpc,
}

fn status_result(status: Option<RpcStatus>) -> Result<(), Status> {
    let status = status.unwrap_or_default();
    if status.code == 0 {
        return Ok(());
    }
    let code = Code::from_i32(status.code);
    let message = status.message.clone();
    Err(Status::with_details(
        code,
        message,
        Bytes::from(status.encode_to_vec()),
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll},
        time::Duration,
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures_core::Stream;
    use googleapis_tonic_google_rpc::google::rpc::{RetryInfo, Status as RpcStatus};
    use prost::Message;
    use prost_types::Any;
    use tokio::sync::Mutex;
    use tonic::{Code, Request, Status};

    use super::{
        BatchPolicy, BulkMutationOptions, BulkMutationResult, MutateRowsService, ResponseStream,
        execute_with_service, repeated_message_size, split_batches, validate_options, varint_size,
    };
    use crate::{
        BulkMutation, BulkMutationPolicyIssue, ClientConfig, DeadlinePolicy, Error, Jitter,
        MutateRowsResponseIssue, Mutation, MutationFailureCause, RowMutation,
        proto::{
            MutateRowsRequest, MutateRowsResponse, mutate_rows_response::Entry as ResponseEntry,
        },
        retry::RetryPolicy,
    };

    enum Script {
        StartError(Status),
        Stream(Vec<Result<MutateRowsResponse, Status>>),
        SuccessAll,
        Pending,
        PendingDrop(Arc<AtomicBool>),
    }

    struct CapturedRequest {
        message: MutateRowsRequest,
        routing: String,
        has_timeout: bool,
    }

    #[derive(Default)]
    struct FakeService {
        scripts: Mutex<VecDeque<Script>>,
        requests: Mutex<Vec<CapturedRequest>>,
    }

    impl FakeService {
        fn new(scripts: impl IntoIterator<Item = Script>) -> Self {
            Self {
                scripts: Mutex::new(scripts.into_iter().collect()),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl MutateRowsService for FakeService {
        async fn mutate_rows(
            &self,
            request: Request<MutateRowsRequest>,
        ) -> Result<ResponseStream, Status> {
            let routing = request
                .metadata()
                .get("x-goog-request-params")
                .expect("routing metadata")
                .to_str()
                .expect("ASCII routing")
                .to_owned();
            let has_timeout = request.metadata().contains_key("grpc-timeout");
            let message = request.into_inner();
            let entry_count = message.entries.len();
            self.requests.lock().await.push(CapturedRequest {
                message,
                routing,
                has_timeout,
            });
            match self
                .scripts
                .lock()
                .await
                .pop_front()
                .expect("a scripted attempt")
            {
                Script::StartError(status) => Err(status),
                Script::Stream(items) => Ok(Box::pin(tokio_stream::iter(items))),
                Script::SuccessAll => Ok(Box::pin(tokio_stream::iter([Ok(response(
                    (0..entry_count).map(|index| {
                        (
                            i64::try_from(index).expect("small test index"),
                            Some(rpc_status(Code::Ok, "")),
                        )
                    }),
                ))]))),
                Script::Pending => Ok(Box::pin(futures_util::stream::pending())),
                Script::PendingDrop(dropped) => Ok(Box::pin(PendingDrop { dropped })),
            }
        }
    }

    struct PendingDrop {
        dropped: Arc<AtomicBool>,
    }

    impl Stream for PendingDrop {
        type Item = Result<MutateRowsResponse, Status>;

        fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl Drop for PendingDrop {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    fn config() -> ClientConfig {
        ClientConfig::new("project", "instance")
            .expect("valid config")
            .with_app_profile_id("analytics")
            .expect("valid app profile")
    }

    fn options() -> BulkMutationOptions {
        BulkMutationOptions {
            retry: RetryPolicy {
                max_attempts: 3,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(4),
                multiplier: 2.0,
                jitter: Jitter::None,
            },
            deadlines: DeadlinePolicy {
                operation_timeout: Duration::from_secs(10),
                attempt_timeout: Duration::from_secs(2),
            },
            batch: BatchPolicy {
                max_entries_per_request: 100,
                max_request_bytes: 20 * 1024 * 1024,
                max_in_flight_requests: 1,
            },
        }
    }

    fn safe_row(key: impl Into<Bytes>) -> RowMutation {
        RowMutation::new(key)
            .expect("valid row")
            .mutation(
                Mutation::set_cell_at("cf", b"q".to_vec(), 1_000, b"value".to_vec())
                    .expect("valid cell"),
            )
            .expect("within mutation limit")
    }

    fn unsafe_row(key: impl Into<Bytes>) -> RowMutation {
        RowMutation::new(key)
            .expect("valid row")
            .mutation(
                Mutation::set_cell_at_server_time("cf", b"q".to_vec(), b"value".to_vec())
                    .expect("valid cell"),
            )
            .expect("within mutation limit")
    }

    fn bulk(rows: impl IntoIterator<Item = RowMutation>) -> BulkMutation {
        let mut mutation = BulkMutation::new("events").expect("valid table");
        for row in rows {
            mutation.push(row).expect("nonempty row");
        }
        mutation
    }

    fn response(entries: impl IntoIterator<Item = (i64, Option<RpcStatus>)>) -> MutateRowsResponse {
        MutateRowsResponse {
            entries: entries
                .into_iter()
                .map(|(index, status)| ResponseEntry { index, status })
                .collect(),
            ..MutateRowsResponse::default()
        }
    }

    fn rpc_status(code: Code, message: &str) -> RpcStatus {
        RpcStatus {
            code: i32::from(code),
            message: message.to_owned(),
            details: Vec::new(),
        }
    }

    fn rich_retry_status(delay: Duration) -> RpcStatus {
        let retry_info = RetryInfo {
            retry_delay: Some(prost_types::Duration {
                seconds: i64::try_from(delay.as_secs()).expect("small delay"),
                nanos: i32::try_from(delay.subsec_nanos()).expect("valid nanos"),
            }),
        };
        RpcStatus {
            code: i32::from(Code::Unavailable),
            message: "retry later".to_owned(),
            details: vec![Any {
                type_url: "type.googleapis.com/google.rpc.RetryInfo".to_owned(),
                value: retry_info.encode_to_vec(),
            }],
        }
    }

    #[test]
    fn defaults_match_google_bulk_write_guidance() {
        let options = BulkMutationOptions::default();

        assert_eq!(options.batch.max_entries_per_request, 100);
        assert_eq!(options.batch.max_request_bytes, 20 * 1024 * 1024);
        assert_eq!(options.batch.max_in_flight_requests, 5);
        assert_eq!(
            options.deadlines.operation_timeout,
            Duration::from_secs(600)
        );
        assert_eq!(options.deadlines.attempt_timeout, Duration::from_secs(60));
        assert_eq!(options.retry, RetryPolicy::default());
    }

    #[test]
    fn result_accessors_report_operation_counts() {
        let result = BulkMutationResult {
            entries: 12,
            rpc_attempts: 3,
            request_batches: 2,
        };

        assert_eq!(result.entries(), 12);
        assert_eq!(result.rpc_attempts(), 3);
        assert_eq!(result.request_batches(), 2);
    }

    #[test]
    fn encoded_message_size_includes_tag_and_varint() {
        assert_eq!(varint_size(0), 1);
        assert_eq!(varint_size(127), 1);
        assert_eq!(varint_size(128), 2);
        assert_eq!(repeated_message_size(127), 129);
        assert_eq!(repeated_message_size(128), 131);
    }

    #[test]
    fn custom_options_are_plain_owned_values() {
        let options = BulkMutationOptions {
            retry: RetryPolicy {
                max_attempts: 2,
                ..RetryPolicy::default()
            },
            deadlines: DeadlinePolicy {
                operation_timeout: Duration::from_secs(5),
                attempt_timeout: Duration::from_secs(1),
            },
            batch: BatchPolicy {
                max_entries_per_request: 10,
                max_request_bytes: 1024,
                max_in_flight_requests: 2,
            },
        };

        assert_eq!(options.clone(), options);
    }

    #[tokio::test(start_paused = true)]
    async fn successful_entries_can_arrive_out_of_order_across_messages() {
        let service = Arc::new(FakeService::new([Script::Stream(vec![
            Ok(response([(2, Some(rpc_status(Code::Ok, "")))])),
            Ok(response([(0, None), (1, Some(rpc_status(Code::Ok, "")))])),
        ])]));
        let result = execute_with_service(
            service.clone(),
            &config(),
            bulk([
                safe_row(b"a".to_vec()),
                safe_row(b"b".to_vec()),
                safe_row(b"c".to_vec()),
            ]),
            options(),
        )
        .await
        .expect("all entries succeed");

        assert_eq!(result.entries(), 3);
        assert_eq!(result.rpc_attempts(), 1);
        assert_eq!(result.request_batches(), 1);
        let requests = service.requests.lock().await;
        assert_eq!(
            requests[0].message.table_name,
            "projects/project/instances/instance/tables/events"
        );
        assert_eq!(requests[0].message.app_profile_id, "analytics");
        assert_eq!(
            requests[0].routing,
            "table_name=projects/project/instances/instance/tables/events"
        );
        assert!(requests[0].has_timeout);
    }

    #[tokio::test(start_paused = true)]
    async fn partial_retry_sends_only_transient_entries_and_keeps_original_indexes() {
        let service = Arc::new(FakeService::new([
            Script::Stream(vec![Ok(response([
                (0, Some(rpc_status(Code::Ok, ""))),
                (1, Some(rpc_status(Code::Unavailable, "retry"))),
                (2, Some(rpc_status(Code::InvalidArgument, "bad mutation"))),
            ]))]),
            Script::SuccessAll,
        ]));
        let error = execute_with_service(
            service.clone(),
            &config(),
            bulk([
                safe_row(b"a".to_vec()),
                safe_row(b"b".to_vec()),
                safe_row(b"c".to_vec()),
            ]),
            options(),
        )
        .await
        .expect_err("one permanent failure");
        let Error::BulkMutation(error) = error else {
            panic!("grouped mutation error");
        };

        assert_eq!(error.total_entries(), 3);
        assert_eq!(error.successful_entries(), 2);
        assert_eq!(error.rpc_attempts(), 2);
        assert_eq!(error.failures().len(), 1);
        assert_eq!(error.failures()[0].index(), 2);
        assert_eq!(error.failures()[0].attempts(), 1);
        assert!(matches!(
            error.failures()[0].cause(),
            MutationFailureCause::EntryStatus { status, .. }
                if status.code() == Code::InvalidArgument
        ));
        let requests = service.requests.lock().await;
        assert_eq!(requests[1].message.entries.len(), 1);
        assert_eq!(requests[1].message.entries[0].row_key.as_ref(), b"b");
    }

    #[tokio::test(start_paused = true)]
    async fn stream_failure_retries_only_entries_without_results() {
        let service = Arc::new(FakeService::new([
            Script::Stream(vec![
                Ok(response([(0, Some(rpc_status(Code::Ok, "")))])),
                Err(Status::unavailable("stream broke")),
            ]),
            Script::SuccessAll,
        ]));
        let result = execute_with_service(
            service.clone(),
            &config(),
            bulk([
                safe_row(b"a".to_vec()),
                safe_row(b"b".to_vec()),
                safe_row(b"c".to_vec()),
            ]),
            options(),
        )
        .await
        .expect("unresolved entries retry");

        assert_eq!(result.entries(), 3);
        assert_eq!(result.rpc_attempts(), 2);
        let requests = service.requests.lock().await;
        assert_eq!(requests[1].message.entries.len(), 2);
        assert_eq!(requests[1].message.entries[0].row_key.as_ref(), b"b");
        assert_eq!(requests[1].message.entries[1].row_key.as_ref(), b"c");
    }

    #[tokio::test(start_paused = true)]
    async fn ambiguous_rpc_failure_does_not_replay_server_time_writes() {
        let service = Arc::new(FakeService::new([
            Script::StartError(Status::unavailable("no response")),
            Script::SuccessAll,
        ]));
        let error = execute_with_service(
            service.clone(),
            &config(),
            bulk([safe_row(b"safe".to_vec()), unsafe_row(b"unsafe".to_vec())]),
            options(),
        )
        .await
        .expect_err("unsafe entry stays failed");
        let Error::BulkMutation(error) = error else {
            panic!("grouped mutation error");
        };

        assert_eq!(error.successful_entries(), 1);
        assert_eq!(error.failures()[0].index(), 1);
        assert!(matches!(
            error.failures()[0].cause(),
            MutationFailureCause::RpcStatus {
                status,
                retry_safe: false
            } if status.code() == Code::Unavailable
        ));
        let requests = service.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].message.entries.len(), 1);
        assert_eq!(requests[1].message.entries[0].row_key.as_ref(), b"safe");
    }

    #[tokio::test(start_paused = true)]
    async fn retry_stops_at_the_attempt_limit() {
        let service = Arc::new(FakeService::new([
            Script::StartError(Status::unavailable("first")),
            Script::Stream(vec![Ok(response([(
                0,
                Some(rpc_status(Code::Unavailable, "second")),
            )]))]),
        ]));
        let mut limited = options();
        limited.retry.max_attempts = 2;
        let error = execute_with_service(
            service.clone(),
            &config(),
            bulk([safe_row(b"row".to_vec())]),
            limited,
        )
        .await
        .expect_err("attempts exhausted");
        let Error::BulkMutation(error) = error else {
            panic!("grouped mutation error");
        };

        assert_eq!(error.rpc_attempts(), 2);
        assert_eq!(error.failures()[0].attempts(), 2);
        assert!(matches!(
            error.failures()[0].cause(),
            MutationFailureCause::EntryStatus { status, .. }
                if status.code() == Code::Unavailable
        ));
        assert_eq!(service.requests.lock().await.len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_info_delay_is_honored_for_entry_statuses() {
        let delay = Duration::from_secs(5);
        let service = Arc::new(FakeService::new([
            Script::Stream(vec![Ok(response([(0, Some(rich_retry_status(delay)))]))]),
            Script::SuccessAll,
        ]));
        let started = tokio::time::Instant::now();
        let result = execute_with_service(
            service,
            &config(),
            bulk([safe_row(b"row".to_vec())]),
            options(),
        )
        .await
        .expect("retry succeeds");

        assert_eq!(result.rpc_attempts(), 2);
        assert!(tokio::time::Instant::now().duration_since(started) >= delay);
    }

    #[tokio::test(start_paused = true)]
    async fn attempt_timeout_retries_and_operation_timeout_stops_work() {
        let retrying = Arc::new(FakeService::new([Script::Pending, Script::SuccessAll]));
        let mut timed = options();
        timed.deadlines.attempt_timeout = Duration::from_secs(1);
        let result = execute_with_service(
            retrying.clone(),
            &config(),
            bulk([safe_row(b"row".to_vec())]),
            timed,
        )
        .await
        .expect("attempt timeout retries");
        assert_eq!(result.rpc_attempts(), 2);

        let expiring = Arc::new(FakeService::new([
            Script::Pending,
            Script::Pending,
            Script::Pending,
        ]));
        let mut short = options();
        short.deadlines.attempt_timeout = Duration::from_secs(1);
        short.deadlines.operation_timeout = Duration::from_millis(1_500);
        let error = execute_with_service(
            expiring,
            &config(),
            bulk([safe_row(b"row".to_vec())]),
            short,
        )
        .await
        .expect_err("operation deadline");
        let Error::BulkMutation(error) = error else {
            panic!("grouped mutation error");
        };
        assert!(matches!(
            error.failures()[0].cause(),
            MutationFailureCause::DeadlineExceeded { timeout }
                if *timeout == Duration::from_millis(1_500)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn malformed_response_indexes_are_typed_and_not_retried() {
        let cases = [
            (
                response([(-1, Some(rpc_status(Code::Ok, "")))]),
                MutateRowsResponseIssue::NegativeIndex { index: -1 },
            ),
            (
                response([(2, Some(rpc_status(Code::Ok, "")))]),
                MutateRowsResponseIssue::IndexOutOfRange {
                    index: 2,
                    entry_count: 1,
                },
            ),
            (
                response([
                    (0, Some(rpc_status(Code::Ok, ""))),
                    (0, Some(rpc_status(Code::Ok, ""))),
                ]),
                MutateRowsResponseIssue::DuplicateIndex { index: 0 },
            ),
        ];

        for (wire_response, expected) in cases {
            let service = Arc::new(FakeService::new([Script::Stream(vec![Ok(wire_response)])]));
            let error = execute_with_service(
                service.clone(),
                &config(),
                bulk([safe_row(b"row".to_vec())]),
                options(),
            )
            .await
            .expect_err("invalid response");
            let Error::BulkMutation(error) = error else {
                panic!("grouped mutation error");
            };
            assert!(matches!(
                error.failures()[0].cause(),
                MutationFailureCause::InvalidResponse { issue } if *issue == expected
            ));
            assert_eq!(service.requests.lock().await.len(), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn missing_response_entry_is_a_typed_failure() {
        let service = Arc::new(FakeService::new([Script::Stream(vec![Ok(response([(
            0,
            Some(rpc_status(Code::Ok, "")),
        )]))])]));
        let error = execute_with_service(
            service,
            &config(),
            bulk([safe_row(b"a".to_vec()), safe_row(b"b".to_vec())]),
            options(),
        )
        .await
        .expect_err("second entry was not reported");
        let Error::BulkMutation(error) = error else {
            panic!("grouped mutation error");
        };

        assert_eq!(error.successful_entries(), 1);
        assert_eq!(error.failures()[0].index(), 1);
        assert!(matches!(
            error.failures()[0].cause(),
            MutationFailureCause::InvalidResponse {
                issue: MutateRowsResponseIssue::MissingIndex { index: 1 }
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_rpc_and_entry_statuses_preserve_rich_details() {
        let detail = Any {
            type_url: "example.test/detail".to_owned(),
            value: vec![1, 2, 3],
        };
        let entry_status = RpcStatus {
            code: i32::from(Code::InvalidArgument),
            message: "bad".to_owned(),
            details: vec![detail],
        };
        let service = Arc::new(FakeService::new([Script::Stream(vec![Ok(response([(
            0,
            Some(entry_status.clone()),
        )]))])]));
        let error = execute_with_service(
            service,
            &config(),
            bulk([safe_row(b"row".to_vec())]),
            options(),
        )
        .await
        .expect_err("entry fails");
        let Error::BulkMutation(error) = error else {
            panic!("grouped mutation error");
        };
        let status = error.failures()[0]
            .cause()
            .status()
            .expect("status is preserved");
        assert_eq!(
            RpcStatus::decode(status.details()).expect("rich status"),
            entry_status
        );

        let service = Arc::new(FakeService::new([Script::StartError(
            Status::permission_denied("denied"),
        )]));
        let error = execute_with_service(
            service,
            &config(),
            bulk([safe_row(b"row".to_vec())]),
            options(),
        )
        .await
        .expect_err("RPC fails");
        let Error::BulkMutation(error) = error else {
            panic!("grouped mutation error");
        };
        assert!(matches!(
            error.failures()[0].cause(),
            MutationFailureCause::RpcStatus { status, .. }
                if status.code() == Code::PermissionDenied
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn empty_operation_is_a_noop() {
        let service = Arc::new(FakeService::default());
        let result = execute_with_service(
            service.clone(),
            &config(),
            BulkMutation::new("events").expect("valid table"),
            options(),
        )
        .await
        .expect("empty operation succeeds");

        assert_eq!(result, BulkMutationResult::default());
        assert!(service.requests.lock().await.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn batching_splits_by_entry_count_and_encoded_bytes() {
        let count_service = Arc::new(FakeService::new([
            Script::SuccessAll,
            Script::SuccessAll,
            Script::SuccessAll,
        ]));
        let mut count_options = options();
        count_options.batch.max_entries_per_request = 2;
        let result = execute_with_service(
            count_service.clone(),
            &config(),
            bulk((0..5).map(|index| safe_row(format!("row-{index}").into_bytes()))),
            count_options,
        )
        .await
        .expect("all batches succeed");
        assert_eq!(result.request_batches(), 3);
        assert_eq!(
            count_service
                .requests
                .lock()
                .await
                .iter()
                .map(|request| request.message.entries.len())
                .collect::<Vec<_>>(),
            vec![2, 2, 1]
        );

        let byte_service = Arc::new(FakeService::new([
            Script::SuccessAll,
            Script::SuccessAll,
            Script::SuccessAll,
        ]));
        let mut byte_options = options();
        byte_options.batch.max_request_bytes = 1;
        let result = execute_with_service(
            byte_service,
            &config(),
            bulk([
                safe_row(b"a".to_vec()),
                safe_row(b"b".to_vec()),
                safe_row(b"c".to_vec()),
            ]),
            byte_options,
        )
        .await
        .expect("oversized entries run alone");
        assert_eq!(result.request_batches(), 3);
    }

    #[test]
    fn batching_never_exceeds_the_api_mutation_count() {
        let many = |key: &'static [u8], count| {
            let mut row = RowMutation::new(key.to_vec()).expect("valid row");
            for _ in 0..count {
                row = row
                    .mutation(Mutation::delete_row())
                    .expect("within row limit");
            }
            row
        };
        let batches = split_batches(
            vec![many(b"a", 50_001), many(b"b", 50_000)],
            "projects/p/instances/i/tables/t",
            "default",
            &BatchPolicy {
                max_entries_per_request: 100,
                max_request_bytes: usize::MAX,
                max_in_flight_requests: 1,
            },
        );

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0][0].proto.mutations.len(), 50_001);
        assert_eq!(batches[1][0].proto.mutations.len(), 50_000);
    }

    #[tokio::test]
    async fn in_flight_limit_is_bounded_and_cancellation_drops_streams() {
        let first_dropped = Arc::new(AtomicBool::new(false));
        let second_dropped = Arc::new(AtomicBool::new(false));
        let service = Arc::new(FakeService::new([
            Script::PendingDrop(first_dropped.clone()),
            Script::PendingDrop(second_dropped.clone()),
        ]));
        let mut bounded = options();
        bounded.batch.max_entries_per_request = 1;
        bounded.batch.max_in_flight_requests = 2;
        let task_service = service.clone();
        let task = tokio::spawn(async move {
            execute_with_service(
                task_service,
                &config(),
                bulk((0..4).map(|index| safe_row(format!("row-{index}").into_bytes()))),
                bounded,
            )
            .await
        });

        for _ in 0..10 {
            if service.requests.lock().await.len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(service.requests.lock().await.len(), 2);
        task.abort();
        let _ = task.await;
        tokio::task::yield_now().await;

        assert!(first_dropped.load(Ordering::SeqCst));
        assert!(second_dropped.load(Ordering::SeqCst));
        assert_eq!(service.requests.lock().await.len(), 2);
    }

    #[test]
    fn invalid_retry_and_deadline_values_return_typed_issues() {
        let cases = [
            (
                BulkMutationOptions {
                    retry: RetryPolicy {
                        max_attempts: 0,
                        ..RetryPolicy::default()
                    },
                    ..BulkMutationOptions::default()
                },
                BulkMutationPolicyIssue::ZeroMaxAttempts,
            ),
            (
                BulkMutationOptions {
                    retry: RetryPolicy {
                        initial_backoff: Duration::ZERO,
                        ..RetryPolicy::default()
                    },
                    ..BulkMutationOptions::default()
                },
                BulkMutationPolicyIssue::ZeroInitialBackoff,
            ),
            (
                BulkMutationOptions {
                    retry: RetryPolicy {
                        max_backoff: Duration::ZERO,
                        ..RetryPolicy::default()
                    },
                    ..BulkMutationOptions::default()
                },
                BulkMutationPolicyIssue::ZeroMaxBackoff,
            ),
            (
                BulkMutationOptions {
                    retry: RetryPolicy {
                        initial_backoff: Duration::from_secs(2),
                        max_backoff: Duration::from_secs(1),
                        ..RetryPolicy::default()
                    },
                    ..BulkMutationOptions::default()
                },
                BulkMutationPolicyIssue::MaxBackoffTooSmall,
            ),
            (
                BulkMutationOptions {
                    retry: RetryPolicy {
                        multiplier: f64::NAN,
                        ..RetryPolicy::default()
                    },
                    ..BulkMutationOptions::default()
                },
                BulkMutationPolicyIssue::InvalidBackoffMultiplier,
            ),
            (
                BulkMutationOptions {
                    deadlines: DeadlinePolicy {
                        operation_timeout: Duration::ZERO,
                        ..DeadlinePolicy::default()
                    },
                    ..BulkMutationOptions::default()
                },
                BulkMutationPolicyIssue::ZeroOperationTimeout,
            ),
            (
                BulkMutationOptions {
                    deadlines: DeadlinePolicy {
                        attempt_timeout: Duration::ZERO,
                        ..DeadlinePolicy::default()
                    },
                    ..BulkMutationOptions::default()
                },
                BulkMutationPolicyIssue::ZeroAttemptTimeout,
            ),
        ];

        for (invalid, expected) in cases {
            assert_policy_issue(&invalid, expected);
        }
    }

    #[test]
    fn invalid_batch_values_return_typed_issues() {
        let cases = [
            (
                BulkMutationOptions {
                    batch: BatchPolicy {
                        max_entries_per_request: 0,
                        ..BatchPolicy::default()
                    },
                    ..BulkMutationOptions::default()
                },
                BulkMutationPolicyIssue::ZeroEntriesPerRequest,
            ),
            (
                BulkMutationOptions {
                    batch: BatchPolicy {
                        max_request_bytes: 0,
                        ..BatchPolicy::default()
                    },
                    ..BulkMutationOptions::default()
                },
                BulkMutationPolicyIssue::ZeroRequestBytes,
            ),
            (
                BulkMutationOptions {
                    batch: BatchPolicy {
                        max_in_flight_requests: 0,
                        ..BatchPolicy::default()
                    },
                    ..BulkMutationOptions::default()
                },
                BulkMutationPolicyIssue::ZeroInFlightRequests,
            ),
        ];

        for (invalid, expected) in cases {
            assert_policy_issue(&invalid, expected);
        }
    }

    fn assert_policy_issue(options: &BulkMutationOptions, expected: BulkMutationPolicyIssue) {
        assert!(matches!(
            validate_options(options),
            Err(Error::InvalidBulkMutationPolicy { issue }) if issue == expected
        ));
    }
}
