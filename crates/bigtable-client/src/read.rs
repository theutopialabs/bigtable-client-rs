use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use tokio::{
    sync::mpsc,
    time::{Instant, timeout, timeout_at},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Code, Request, Status,
    metadata::{Ascii, MetadataValue},
};
use tracing::Instrument;

use crate::{
    ClientConfig, Error, Query, RawClient, ReadOptions, Row,
    merge::RowMerger,
    proto::{
        ReadRowsRequest, ReadRowsResponse, RowSet,
        row_range::{EndKey, StartKey},
    },
    retry,
    telemetry::{
        AttemptSummary, AttemptTracker, BigtableOperation, OperationHandle, OperationSummary,
        OperationTracker, Telemetry,
    },
};

const STREAM_BUFFER: usize = 16;

type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<ReadRowsResponse, Status>> + Send + 'static>>;

/// A stream of complete Bigtable rows.
#[must_use = "row streams do nothing unless they are consumed"]
pub struct RowStream {
    inner: ReceiverStream<Result<Row, Error>>,
}

impl RowStream {
    /// Waits for the next row or terminal stream error.
    pub async fn next(&mut self) -> Option<Result<Row, Error>> {
        self.inner.next().await
    }
}

impl Stream for RowStream {
    type Item = Result<Row, Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(context)
    }
}

impl std::fmt::Debug for RowStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("RowStream").finish_non_exhaustive()
    }
}

#[async_trait]
trait ReadRowsService: Send + Sync {
    async fn read_rows(&self, request: Request<ReadRowsRequest>) -> Result<ResponseStream, Status>;
}

struct TonicReadRowsService {
    raw: RawClient,
}

#[async_trait]
impl ReadRowsService for TonicReadRowsService {
    async fn read_rows(&self, request: Request<ReadRowsRequest>) -> Result<ResponseStream, Status> {
        let mut raw = self.raw.clone();
        let stream = raw.read_rows(request).await?.into_inner();
        Ok(Box::pin(stream))
    }
}

pub(crate) async fn start(
    raw: RawClient,
    config: &ClientConfig,
    telemetry: Telemetry,
    query: Query,
    options: ReadOptions,
) -> Result<RowStream, Error> {
    start_with_service_and_telemetry(
        Arc::new(TonicReadRowsService { raw }),
        config,
        telemetry,
        query,
        options,
    )
    .await
}

async fn start_with_service_and_telemetry(
    service: Arc<dyn ReadRowsService>,
    config: &ClientConfig,
    telemetry: Telemetry,
    query: Query,
    options: ReadOptions,
) -> Result<RowStream, Error> {
    retry::validate(&options)?;
    query.validate()?;
    let table_id = query.table_id().to_owned();
    let request = query.into_request(config);
    let routing = format!("table_name={}", request.table_name)
        .parse()
        .map_err(|source| Error::InvalidClientMetadata {
            header: "x-goog-request-params",
            source,
        })?;
    let deadline = Instant::now()
        .checked_add(options.deadlines.operation_timeout)
        .ok_or_else(|| Error::invalid_read_policy(crate::ReadPolicyIssue::DeadlineTooLarge))?;
    let operation_telemetry = telemetry.operation(config, BigtableOperation::ReadRows, table_id, 0);
    let mut operation = ReadOperation::new(
        service,
        request,
        routing,
        options,
        deadline,
        operation_telemetry,
    );
    let active = match operation.open(None, None).await? {
        OpenResult::Active(active) => active,
        OpenResult::Complete | OpenResult::Cancelled => {
            let (_sender, receiver) = mpsc::channel(1);
            return Ok(RowStream {
                inner: ReceiverStream::new(receiver),
            });
        }
    };
    let (sender, receiver) = mpsc::channel(STREAM_BUFFER);
    tokio::spawn(operation.run(active, sender));

    Ok(RowStream {
        inner: ReceiverStream::new(receiver),
    })
}

#[cfg(test)]
async fn start_with_service(
    service: Arc<dyn ReadRowsService>,
    config: &ClientConfig,
    query: Query,
    options: ReadOptions,
) -> Result<RowStream, Error> {
    start_with_service_and_telemetry(
        service,
        config,
        Telemetry::from_observer(None),
        query,
        options,
    )
    .await
}

struct ReadOperation {
    service: Arc<dyn ReadRowsService>,
    original: ReadRowsRequest,
    routing: MetadataValue<Ascii>,
    options: ReadOptions,
    deadline: Instant,
    attempts: u32,
    rows_returned: i64,
    progress: Option<Bytes>,
    merger: RowMerger,
    telemetry: OperationTracker,
}

struct ActiveAttempt {
    stream: ResponseStream,
    deadline: Instant,
    rows: u64,
    telemetry: AttemptTracker,
}

impl ActiveAttempt {
    fn finish(&mut self, code: Code) {
        self.telemetry.finish(
            code,
            AttemptSummary {
                rows: self.rows,
                ..AttemptSummary::default()
            },
        );
    }
}

enum OpenResult {
    Active(ActiveAttempt),
    Complete,
    Cancelled,
}

impl ReadOperation {
    fn new(
        service: Arc<dyn ReadRowsService>,
        original: ReadRowsRequest,
        routing: MetadataValue<Ascii>,
        options: ReadOptions,
        deadline: Instant,
        telemetry: OperationTracker,
    ) -> Self {
        let reversed = original.reversed;

        Self {
            service,
            original,
            routing,
            options,
            deadline,
            attempts: 0,
            rows_returned: 0,
            progress: None,
            merger: RowMerger::new(reversed),
            telemetry,
        }
    }

    fn telemetry(&self) -> OperationHandle {
        self.telemetry.handle()
    }

    async fn run(mut self, mut active: ActiveAttempt, sender: mpsc::Sender<Result<Row, Error>>) {
        loop {
            let message_deadline = active.deadline.min(self.deadline);
            let next = tokio::select! {
                () = sender.closed() => {
                    active.finish(Code::Cancelled);
                    self.finish(Code::Cancelled);
                    return;
                },
                next = timeout_at(message_deadline, active.stream.next())
                    .instrument(active.telemetry.span()) => next,
            };
            match next {
                Ok(Some(Ok(response))) => {
                    active.telemetry.first_response();
                    let before = self.rows_returned;
                    match self.handle_response(response, &sender).await {
                        Ok(true) => {
                            active.rows = active.rows.saturating_add(
                                u64::try_from(self.rows_returned.saturating_sub(before))
                                    .unwrap_or(u64::MAX),
                            );
                            if self.limit_reached() {
                                active.finish(Code::Ok);
                                self.finish(Code::Ok);
                                return;
                            }
                        }
                        Ok(false) => {
                            active.finish(Code::Cancelled);
                            self.finish(Code::Cancelled);
                            return;
                        }
                        Err(error) => {
                            active.finish(Code::Unknown);
                            self.finish(Code::Unknown);
                            send_terminal(&sender, error).await;
                            return;
                        }
                    }
                }
                Ok(None) => {
                    active.finish(Code::Ok);
                    if let Err(issue) = self.merger.finish() {
                        self.finish(Code::Unknown);
                        send_terminal(&sender, Error::InvalidReadRowsResponse { issue }).await;
                    } else {
                        self.finish(Code::Ok);
                    }
                    return;
                }
                Ok(Some(Err(status))) => {
                    active.finish(status.code());
                    self.merger.discard_partial();
                    match self.open(Some(status), Some(&sender)).await {
                        Ok(OpenResult::Active(next_attempt)) => active = next_attempt,
                        Ok(OpenResult::Complete | OpenResult::Cancelled) => return,
                        Err(error) => {
                            send_terminal(&sender, error).await;
                            return;
                        }
                    }
                }
                Err(_) => {
                    active.finish(Code::DeadlineExceeded);
                    self.merger.discard_partial();
                    let status = Status::deadline_exceeded("ReadRows attempt deadline exceeded");
                    match self.open(Some(status), Some(&sender)).await {
                        Ok(OpenResult::Active(next_attempt)) => active = next_attempt,
                        Ok(OpenResult::Complete | OpenResult::Cancelled) => return,
                        Err(error) => {
                            send_terminal(&sender, error).await;
                            return;
                        }
                    }
                }
            }
        }
    }

    async fn handle_response(
        &mut self,
        response: ReadRowsResponse,
        sender: &mpsc::Sender<Result<Row, Error>>,
    ) -> Result<bool, Error> {
        if !response.last_scanned_row_key.is_empty() {
            self.merger
                .scan_marker(response.last_scanned_row_key.clone())
                .map_err(|issue| Error::InvalidReadRowsResponse { issue })?;
            self.progress = Some(response.last_scanned_row_key);
        }

        for chunk in response.chunks {
            if let Some(row) = self
                .merger
                .push(&chunk)
                .map_err(|issue| Error::InvalidReadRowsResponse { issue })?
            {
                self.rows_returned += 1;
                self.progress = Some(row.key.clone());
                let blocked = Instant::now();
                let sent = sender.send(Ok(row)).await;
                self.telemetry().application_blocked(blocked.elapsed());
                if sent.is_err() {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    async fn open(
        &mut self,
        mut failure: Option<Status>,
        sender: Option<&mpsc::Sender<Result<Row, Error>>>,
    ) -> Result<OpenResult, Error> {
        loop {
            if Instant::now() >= self.deadline {
                self.finish(Code::DeadlineExceeded);
                return Err(self.deadline_error());
            }
            if let Some(status) = failure.take() {
                if !retry::is_retryable(&status) || self.attempts >= self.options.retry.max_attempts
                {
                    self.finish(status.code());
                    return Err(Error::ReadRows {
                        attempts: self.attempts,
                        source: status,
                    });
                }
                let delay = retry::backoff(&self.options.retry, self.attempts)
                    .max(retry::retry_delay(&status).unwrap_or(Duration::ZERO));
                let remaining = self.remaining()?;
                if delay >= remaining {
                    self.finish(Code::DeadlineExceeded);
                    return Err(self.deadline_error());
                }
                self.telemetry()
                    .retry(None, self.attempts, status.code(), delay, 0);
                if let Some(sender) = sender {
                    tokio::select! {
                        () = sender.closed() => {
                            self.finish(Code::Cancelled);
                            return Ok(OpenResult::Cancelled);
                        },
                        () = tokio::time::sleep(delay) => {}
                    }
                } else {
                    tokio::time::sleep(delay).await;
                }
            }

            let Some(message) =
                resume_request(&self.original, self.progress.as_deref(), self.rows_returned)
            else {
                self.finish(Code::Ok);
                return Ok(OpenResult::Complete);
            };
            let remaining = self.remaining()?;
            let attempt_timeout = self.options.deadlines.attempt_timeout.min(remaining);
            let attempt_deadline =
                Instant::now().checked_add(attempt_timeout).ok_or_else(|| {
                    Error::invalid_read_policy(crate::ReadPolicyIssue::DeadlineTooLarge)
                })?;
            let mut request = Request::new(message);
            request
                .metadata_mut()
                .insert("x-goog-request-params", self.routing.clone());
            request.set_timeout(attempt_timeout);
            self.attempts += 1;

            let mut telemetry = self
                .telemetry()
                .attempt(None, self.attempts, 0, attempt_timeout);
            let attempt = timeout(attempt_timeout, self.service.read_rows(request))
                .instrument(telemetry.span());
            let result = if let Some(sender) = sender {
                tokio::select! {
                    () = sender.closed() => {
                        telemetry.finish(Code::Cancelled, AttemptSummary::default());
                        self.finish(Code::Cancelled);
                        return Ok(OpenResult::Cancelled);
                    },
                    result = attempt => result,
                }
            } else {
                attempt.await
            };
            match result {
                Ok(Ok(stream)) => {
                    return Ok(OpenResult::Active(ActiveAttempt {
                        stream,
                        deadline: attempt_deadline,
                        rows: 0,
                        telemetry,
                    }));
                }
                Ok(Err(status)) => {
                    telemetry.finish(status.code(), AttemptSummary::default());
                    failure = Some(status);
                }
                Err(_) => {
                    telemetry.finish(Code::DeadlineExceeded, AttemptSummary::default());
                    failure = Some(Status::deadline_exceeded(
                        "ReadRows attempt deadline exceeded",
                    ));
                }
            }
        }
    }

    fn remaining(&mut self) -> Result<Duration, Error> {
        let remaining = self
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero());
        if let Some(remaining) = remaining {
            Ok(remaining)
        } else {
            self.finish(Code::DeadlineExceeded);
            Err(self.deadline_error())
        }
    }

    fn deadline_error(&self) -> Error {
        Error::ReadDeadlineExceeded {
            attempts: self.attempts,
            timeout: self.options.deadlines.operation_timeout,
        }
    }

    fn limit_reached(&self) -> bool {
        self.original.rows_limit > 0 && self.rows_returned >= self.original.rows_limit
    }

    fn finish(&mut self, code: Code) {
        self.telemetry.finish(
            code,
            OperationSummary {
                rows: u64::try_from(self.rows_returned).unwrap_or(u64::MAX),
                ..OperationSummary::default()
            },
        );
    }
}

async fn send_terminal(sender: &mpsc::Sender<Result<Row, Error>>, error: Error) {
    let _ = sender.send(Err(error)).await;
}

fn resume_request(
    original: &ReadRowsRequest,
    progress: Option<&[u8]>,
    rows_returned: i64,
) -> Option<ReadRowsRequest> {
    if original.rows_limit > 0 && rows_returned >= original.rows_limit {
        return None;
    }

    let mut request = original.clone();
    if original.rows_limit > 0 {
        request.rows_limit = original.rows_limit - rows_returned;
    }
    let Some(progress) = progress else {
        return Some(request);
    };

    let rows = original.rows.clone().unwrap_or_else(|| RowSet {
        row_keys: Vec::new(),
        row_ranges: vec![crate::proto::RowRange::default()],
    });
    request.rows = trim_row_set(rows, progress, original.reversed);
    request.rows.as_ref()?;
    Some(request)
}

fn trim_row_set(rows: RowSet, progress: &[u8], reversed: bool) -> Option<RowSet> {
    let row_keys = rows
        .row_keys
        .into_iter()
        .filter(|key| {
            if reversed {
                key.as_ref() < progress
            } else {
                key.as_ref() > progress
            }
        })
        .collect();
    let row_ranges = rows
        .row_ranges
        .into_iter()
        .filter_map(|range| trim_range(range, progress, reversed))
        .collect();
    let result = RowSet {
        row_keys,
        row_ranges,
    };
    if result.row_keys.is_empty() && result.row_ranges.is_empty() {
        None
    } else {
        Some(result)
    }
}

fn trim_range(
    mut range: crate::proto::RowRange,
    progress: &[u8],
    reversed: bool,
) -> Option<crate::proto::RowRange> {
    if reversed {
        if start_at_or_after(range.start_key.as_ref(), progress) {
            return None;
        }
        if end_at_or_after(range.end_key.as_ref(), progress) {
            range.end_key = Some(EndKey::EndKeyOpen(Bytes::copy_from_slice(progress)));
        }
    } else {
        if end_at_or_before(range.end_key.as_ref(), progress) {
            return None;
        }
        if start_at_or_before(range.start_key.as_ref(), progress) {
            range.start_key = Some(StartKey::StartKeyOpen(Bytes::copy_from_slice(progress)));
        }
    }
    Some(range)
}

fn start_at_or_before(start: Option<&StartKey>, key: &[u8]) -> bool {
    match start {
        None => true,
        Some(StartKey::StartKeyClosed(start) | StartKey::StartKeyOpen(start)) => {
            start.as_ref() <= key
        }
    }
}

fn start_at_or_after(start: Option<&StartKey>, key: &[u8]) -> bool {
    match start {
        None => false,
        Some(StartKey::StartKeyClosed(start) | StartKey::StartKeyOpen(start)) => {
            start.as_ref() >= key
        }
    }
}

fn end_at_or_before(end: Option<&EndKey>, key: &[u8]) -> bool {
    match end {
        None => false,
        Some(EndKey::EndKeyOpen(end) | EndKey::EndKeyClosed(end)) => end.as_ref() <= key,
    }
}

fn end_at_or_after(end: Option<&EndKey>, key: &[u8]) -> bool {
    match end {
        None => true,
        Some(EndKey::EndKeyOpen(end) | EndKey::EndKeyClosed(end)) => end.as_ref() >= key,
    }
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
    use tokio::sync::Mutex;
    use tonic::{Code, Request, Status};

    use super::{
        ReadRowsService, ResponseStream, resume_request, start_with_service,
        start_with_service_and_telemetry, trim_row_set,
    };
    use crate::{
        ClientConfig, DeadlinePolicy, DiagnosticEvent, Error, Jitter, Query, ReadOptions,
        RetryPolicy, RowBound, RowMergeIssue, RowRange,
        proto::{
            ReadRowsRequest, ReadRowsResponse, RowSet,
            read_rows_response::{CellChunk, cell_chunk::RowStatus},
            row_range::{EndKey, StartKey},
        },
        telemetry::recorded_telemetry,
    };

    enum Script {
        StartError(Status),
        Stream(Vec<Result<ReadRowsResponse, Status>>),
        Pending,
        PendingDrop(Arc<AtomicBool>),
    }

    #[derive(Default)]
    struct FakeService {
        scripts: Mutex<VecDeque<Script>>,
        requests: Mutex<Vec<ReadRowsRequest>>,
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
    impl ReadRowsService for FakeService {
        async fn read_rows(
            &self,
            request: Request<ReadRowsRequest>,
        ) -> Result<ResponseStream, Status> {
            self.requests.lock().await.push(request.into_inner());
            match self
                .scripts
                .lock()
                .await
                .pop_front()
                .expect("a scripted attempt")
            {
                Script::StartError(status) => Err(status),
                Script::Stream(items) => Ok(Box::pin(tokio_stream::iter(items))),
                Script::Pending => Ok(Box::pin(futures_util::stream::pending())),
                Script::PendingDrop(dropped) => Ok(Box::pin(PendingDrop { dropped })),
            }
        }
    }

    struct PendingDrop {
        dropped: Arc<AtomicBool>,
    }

    impl Stream for PendingDrop {
        type Item = Result<ReadRowsResponse, Status>;

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
        ClientConfig::new("project", "instance").expect("valid config")
    }

    fn options() -> ReadOptions {
        ReadOptions {
            retry: RetryPolicy {
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(4),
                jitter: Jitter::None,
                ..RetryPolicy::default()
            },
            deadlines: DeadlinePolicy {
                operation_timeout: Duration::from_secs(10),
                attempt_timeout: Duration::from_secs(2),
            },
        }
    }

    fn row_response(key: &'static [u8], value: &'static [u8]) -> ReadRowsResponse {
        ReadRowsResponse {
            chunks: vec![CellChunk {
                row_key: Bytes::from_static(key),
                family_name: Some("f".to_owned()),
                qualifier: Some(b"q".to_vec()),
                timestamp_micros: 1,
                labels: Vec::new(),
                value: Bytes::from_static(value),
                value_size: 0,
                row_status: Some(RowStatus::CommitRow(true)),
            }],
            ..ReadRowsResponse::default()
        }
    }

    fn partial_response(key: &'static [u8], value: &'static [u8]) -> ReadRowsResponse {
        ReadRowsResponse {
            chunks: vec![CellChunk {
                row_key: Bytes::from_static(key),
                family_name: Some("f".to_owned()),
                qualifier: Some(b"q".to_vec()),
                timestamp_micros: 1,
                labels: Vec::new(),
                value: Bytes::from_static(value),
                value_size: 0,
                row_status: None,
            }],
            ..ReadRowsResponse::default()
        }
    }

    fn owned_row_response(key: Bytes, value: Bytes) -> ReadRowsResponse {
        ReadRowsResponse {
            chunks: vec![CellChunk {
                row_key: key,
                family_name: Some("f".to_owned()),
                qualifier: Some(b"q".to_vec()),
                timestamp_micros: 1,
                labels: Vec::new(),
                value,
                value_size: 0,
                row_status: Some(RowStatus::CommitRow(true)),
            }],
            ..ReadRowsResponse::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn read_stream_merges_chunks_across_response_messages() {
        let service = Arc::new(FakeService::new([Script::Stream(vec![
            Ok(ReadRowsResponse {
                chunks: vec![CellChunk {
                    row_key: Bytes::from_static(b"row"),
                    family_name: Some("f".to_owned()),
                    qualifier: Some(b"q".to_vec()),
                    timestamp_micros: 1,
                    labels: Vec::new(),
                    value: Bytes::from_static(b"ab"),
                    value_size: 4,
                    row_status: None,
                }],
                ..ReadRowsResponse::default()
            }),
            Ok(ReadRowsResponse {
                chunks: vec![CellChunk {
                    value: Bytes::from_static(b"cd"),
                    row_status: Some(RowStatus::CommitRow(true)),
                    ..CellChunk::default()
                }],
                ..ReadRowsResponse::default()
            }),
        ])]));
        let mut stream = start_with_service(
            service,
            &config(),
            Query::new("table").expect("valid query"),
            options(),
        )
        .await
        .expect("read starts");

        let row = stream.next().await.expect("one result").expect("valid row");
        assert_eq!(row.families[0].columns[0].cells[0].value.as_ref(), b"abcd");
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    #[ignore = "run through the dedicated stress test gate"]
    async fn large_stream_keeps_every_row_in_order() {
        const ROWS: usize = 10_000;

        let responses = (0..ROWS)
            .map(|index| {
                Ok(owned_row_response(
                    Bytes::from(format!("row-{index:05}")),
                    Bytes::from(index.to_string()),
                ))
            })
            .collect::<Vec<_>>();
        let service = Arc::new(FakeService::new([Script::Stream(responses)]));
        let mut stream = start_with_service(
            service.clone(),
            &config(),
            Query::new("table").expect("valid query"),
            options(),
        )
        .await
        .expect("read starts");

        for index in 0..ROWS {
            let row = stream.next().await.expect("row result").expect("valid row");
            assert_eq!(row.key, Bytes::from(format!("row-{index:05}")));
            assert_eq!(
                row.families[0].columns[0].cells[0].value,
                Bytes::from(index.to_string())
            );
        }
        assert!(stream.next().await.is_none());
        assert_eq!(service.requests.lock().await.len(), 1);
    }

    #[tokio::test]
    #[ignore = "run through the dedicated stress test gate"]
    async fn large_split_cell_merges_across_many_messages() {
        const CHUNKS: usize = 4_096;
        const CHUNK_BYTES: usize = 256;

        let mut responses = Vec::with_capacity(CHUNKS);
        for index in 0..CHUNKS {
            let first = index == 0;
            let last = index + 1 == CHUNKS;
            responses.push(Ok(ReadRowsResponse {
                chunks: vec![CellChunk {
                    row_key: if first {
                        Bytes::from_static(b"large")
                    } else {
                        Bytes::new()
                    },
                    family_name: first.then(|| "f".to_owned()),
                    qualifier: first.then(|| b"q".to_vec()),
                    timestamp_micros: i64::from(first),
                    labels: Vec::new(),
                    value: Bytes::from(vec![
                        u8::try_from(index % 251).expect("small byte");
                        CHUNK_BYTES
                    ]),
                    value_size: if last {
                        0
                    } else {
                        i32::try_from(CHUNKS * CHUNK_BYTES).expect("small cell")
                    },
                    row_status: last.then_some(RowStatus::CommitRow(true)),
                }],
                ..ReadRowsResponse::default()
            }));
        }
        let service = Arc::new(FakeService::new([Script::Stream(responses)]));
        let mut stream = start_with_service(
            service,
            &config(),
            Query::new("table").expect("valid query"),
            options(),
        )
        .await
        .expect("read starts");

        let row = stream.next().await.expect("row result").expect("valid row");
        let value = &row.families[0].columns[0].cells[0].value;
        assert_eq!(value.len(), CHUNKS * CHUNK_BYTES);
        for index in [0, 1, 1_024, CHUNKS - 1] {
            assert_eq!(
                value[index * CHUNK_BYTES],
                u8::try_from(index % 251).expect("small byte")
            );
        }
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn invalid_query_stops_before_the_first_rpc() {
        let service = Arc::new(FakeService::default());
        let error = start_with_service(
            service.clone(),
            &config(),
            Query::new("table")
                .expect("valid table")
                .row_key(Bytes::new()),
            options(),
        )
        .await
        .expect_err("empty exact key");

        assert!(matches!(
            error,
            Error::InvalidQuery {
                issue: crate::QueryIssue::EmptyRowKey
            }
        ));
        assert!(service.requests.lock().await.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn retry_discards_partial_row_and_resumes_without_duplicates() {
        let service = Arc::new(FakeService::new([
            Script::Stream(vec![
                Ok(row_response(b"a", b"first")),
                Ok(partial_response(b"b", b"discard")),
                Err(Status::unavailable("transient")),
            ]),
            Script::Stream(vec![Ok(row_response(b"b", b"retried"))]),
        ]));
        let query = Query::new("table")
            .expect("valid query")
            .row_range(RowRange::new(RowBound::Unbounded, RowBound::Unbounded));
        let mut stream = start_with_service(service.clone(), &config(), query, options())
            .await
            .expect("read starts");

        let first = stream.next().await.expect("first").expect("valid first");
        let second = stream.next().await.expect("second").expect("valid second");
        assert_eq!(first.key.as_ref(), b"a");
        assert_eq!(second.key.as_ref(), b"b");
        assert_eq!(
            second.families[0].columns[0].cells[0].value.as_ref(),
            b"retried"
        );
        assert!(stream.next().await.is_none());

        let requests = service.requests.lock().await;
        let resumed = &requests[1].rows.as_ref().expect("resumed rows").row_ranges[0];
        assert_eq!(
            resumed.start_key,
            Some(StartKey::StartKeyOpen(Bytes::from_static(b"a")))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn diagnostics_cover_stream_retry_and_final_summary() {
        let service = Arc::new(FakeService::new([
            Script::Stream(vec![
                Ok(row_response(b"a", b"first")),
                Err(Status::unavailable("retry")),
            ]),
            Script::Stream(vec![Ok(row_response(b"b", b"second"))]),
        ]));
        let (telemetry, events) = recorded_telemetry();
        let mut stream = start_with_service_and_telemetry(
            service,
            &config(),
            telemetry,
            Query::new("table").expect("valid query"),
            options(),
        )
        .await
        .expect("read starts");

        assert!(stream.next().await.expect("first row").is_ok());
        assert!(stream.next().await.expect("second row").is_ok());
        assert!(stream.next().await.is_none());

        let events = events.lock().expect("diagnostics lock");
        assert_eq!(events.len(), 9);
        assert!(matches!(
            events[0],
            DiagnosticEvent::OperationStarted {
                table_id: ref table,
                entries: 0,
                ..
            } if table == "table"
        ));
        assert!(matches!(
            events[1],
            DiagnosticEvent::AttemptStarted {
                batch: None,
                attempt: 1,
                entries: 0,
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
                pending_entries: 0,
                ..
            }
        ));
        assert!(matches!(
            events[7],
            DiagnosticEvent::AttemptFinished {
                attempt: 2,
                code: Code::Ok,
                rows: 1,
                ..
            }
        ));
        assert!(matches!(
            events[8],
            DiagnosticEvent::OperationFinished {
                code: Code::Ok,
                attempts: 2,
                retries: 1,
                rows: 2,
                ..
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn scan_marker_trims_filtered_work_and_row_limit() {
        let service = Arc::new(FakeService::new([
            Script::Stream(vec![
                Ok(row_response(b"a", b"first")),
                Ok(ReadRowsResponse {
                    last_scanned_row_key: Bytes::from_static(b"m"),
                    ..ReadRowsResponse::default()
                }),
                Err(Status::aborted("retry")),
            ]),
            Script::Stream(vec![Ok(row_response(b"z", b"last"))]),
        ]));
        let query = Query::new("table")
            .expect("valid query")
            .row_range(RowRange::new(RowBound::Unbounded, RowBound::Unbounded))
            .limit(3)
            .expect("valid limit");
        let mut stream = start_with_service(service.clone(), &config(), query, options())
            .await
            .expect("read starts");

        assert_eq!(
            stream.next().await.expect("first").expect("first row").key,
            Bytes::from_static(b"a")
        );
        assert_eq!(
            stream
                .next()
                .await
                .expect("second")
                .expect("second row")
                .key,
            Bytes::from_static(b"z")
        );
        assert!(stream.next().await.is_none());

        let requests = service.requests.lock().await;
        assert_eq!(requests[1].rows_limit, 2);
        assert_eq!(
            requests[1].rows.as_ref().expect("resumed rows").row_ranges[0].start_key,
            Some(StartKey::StartKeyOpen(Bytes::from_static(b"m")))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn start_errors_retry_and_non_retryable_errors_stop() {
        let retrying = Arc::new(FakeService::new([
            Script::StartError(Status::unavailable("retry")),
            Script::Stream(vec![Ok(row_response(b"a", b"ok"))]),
        ]));
        let mut stream = start_with_service(
            retrying.clone(),
            &config(),
            Query::new("table").expect("valid query"),
            options(),
        )
        .await
        .expect("retry starts");
        assert!(stream.next().await.expect("row").is_ok());
        assert_eq!(retrying.requests.lock().await.len(), 2);

        let stopping = Arc::new(FakeService::new([Script::StartError(
            Status::permission_denied("stop"),
        )]));
        let error = start_with_service(
            stopping.clone(),
            &config(),
            Query::new("table").expect("valid query"),
            options(),
        )
        .await
        .expect_err("permission error");
        assert!(matches!(
            error,
            Error::ReadRows {
                attempts: 1,
                source
            } if source.code() == Code::PermissionDenied
        ));
        assert_eq!(stopping.requests.lock().await.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_stops_after_the_configured_attempt_count() {
        let service = Arc::new(FakeService::new([
            Script::StartError(Status::unavailable("first")),
            Script::StartError(Status::unavailable("second")),
        ]));
        let mut limited = options();
        limited.retry.max_attempts = 2;
        let error = start_with_service(
            service.clone(),
            &config(),
            Query::new("table").expect("valid query"),
            limited,
        )
        .await
        .expect_err("attempts exhausted");

        assert!(matches!(
            error,
            Error::ReadRows {
                attempts: 2,
                source
            } if source.code() == Code::Unavailable
        ));
        assert_eq!(service.requests.lock().await.len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn attempt_and_operation_deadlines_are_enforced() {
        let service = Arc::new(FakeService::new([
            Script::Pending,
            Script::Pending,
            Script::Pending,
        ]));
        let mut timed = options();
        timed.retry.max_attempts = 3;
        timed.deadlines.attempt_timeout = Duration::from_secs(1);
        timed.deadlines.operation_timeout = Duration::from_millis(2_500);
        let mut stream = start_with_service(
            service,
            &config(),
            Query::new("table").expect("valid query"),
            timed,
        )
        .await
        .expect("first stream opens");
        let error = stream
            .next()
            .await
            .expect("terminal error")
            .expect_err("deadline");

        assert!(matches!(error, Error::ReadDeadlineExceeded { .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_wire_response_is_not_retried() {
        let service = Arc::new(FakeService::new([Script::Stream(vec![Ok(
            partial_response(b"row", b"incomplete"),
        )])]));
        let mut stream = start_with_service(
            service.clone(),
            &config(),
            Query::new("table").expect("valid query"),
            options(),
        )
        .await
        .expect("read starts");
        let error = stream
            .next()
            .await
            .expect("terminal error")
            .expect_err("invalid stream");

        assert!(matches!(
            error,
            Error::InvalidReadRowsResponse {
                issue: RowMergeIssue::IncompleteRow
            }
        ));
        assert_eq!(service.requests.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn dropping_row_stream_cancels_the_active_rpc() {
        let dropped = Arc::new(AtomicBool::new(false));
        let service = Arc::new(FakeService::new([Script::PendingDrop(dropped.clone())]));
        let (telemetry, events) = recorded_telemetry();
        let stream = start_with_service_and_telemetry(
            service,
            &config(),
            telemetry,
            Query::new("table").expect("valid query"),
            options(),
        )
        .await
        .expect("read starts");

        drop(stream);
        tokio::task::yield_now().await;

        assert!(dropped.load(Ordering::SeqCst));
        let events = events.lock().expect("diagnostics lock");
        assert!(matches!(
            events.as_slice(),
            [
                DiagnosticEvent::OperationStarted { .. },
                DiagnosticEvent::AttemptStarted { .. },
                DiagnosticEvent::AttemptFinished {
                    code: Code::Cancelled,
                    ..
                },
                DiagnosticEvent::OperationFinished {
                    code: Code::Cancelled,
                    ..
                }
            ]
        ));
    }

    #[test]
    fn forward_resume_trims_keys_and_range_edges() {
        let rows = RowSet {
            row_keys: vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"m"),
                Bytes::from_static(b"z"),
            ],
            row_ranges: vec![
                crate::proto::RowRange {
                    start_key: Some(StartKey::StartKeyClosed(Bytes::from_static(b"a"))),
                    end_key: Some(EndKey::EndKeyClosed(Bytes::from_static(b"m"))),
                },
                crate::proto::RowRange {
                    start_key: Some(StartKey::StartKeyOpen(Bytes::from_static(b"m"))),
                    end_key: None,
                },
            ],
        };
        let trimmed = trim_row_set(rows, b"m", false).expect("remaining rows");

        assert_eq!(trimmed.row_keys, vec![Bytes::from_static(b"z")]);
        assert_eq!(trimmed.row_ranges.len(), 1);
        assert_eq!(
            trimmed.row_ranges[0].start_key,
            Some(StartKey::StartKeyOpen(Bytes::from_static(b"m")))
        );
    }

    #[test]
    fn reverse_resume_trims_keys_and_range_edges() {
        let rows = RowSet {
            row_keys: vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"m"),
                Bytes::from_static(b"z"),
            ],
            row_ranges: vec![
                crate::proto::RowRange {
                    start_key: None,
                    end_key: Some(EndKey::EndKeyClosed(Bytes::from_static(b"m"))),
                },
                crate::proto::RowRange {
                    start_key: Some(StartKey::StartKeyClosed(Bytes::from_static(b"m"))),
                    end_key: None,
                },
            ],
        };
        let trimmed = trim_row_set(rows, b"m", true).expect("remaining rows");

        assert_eq!(trimmed.row_keys, vec![Bytes::from_static(b"a")]);
        assert_eq!(trimmed.row_ranges.len(), 1);
        assert_eq!(
            trimmed.row_ranges[0].end_key,
            Some(EndKey::EndKeyOpen(Bytes::from_static(b"m")))
        );
    }

    #[test]
    fn fulfilled_resume_stops_without_an_extra_request() {
        let request = ReadRowsRequest {
            rows: Some(RowSet {
                row_keys: vec![Bytes::from_static(b"a")],
                row_ranges: Vec::new(),
            }),
            rows_limit: 1,
            ..ReadRowsRequest::default()
        };

        assert!(resume_request(&request, Some(b"a"), 1).is_none());
        assert!(resume_request(&request, Some(b"a"), 0).is_none());
    }
}
