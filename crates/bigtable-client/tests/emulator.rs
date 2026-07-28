//! Bigtable emulator tests.

#![cfg(feature = "emulator-tests")]

#[cfg(feature = "opentelemetry")]
use std::collections::HashSet;
use std::{
    collections::HashMap,
    env, process,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use bigtable_client::{
    BatchPolicy, BigtableOperation, BulkMutation, BulkMutationOptions, Client, ClientConfig,
    DiagnosticEvent, Error, Mutation, Query, RawClient, ReadOptions, Row, RowMappingIssue,
    RowMutation,
    proto::{
        MutateRowRequest, Mutation as ProtoMutation, ReadRowsRequest, RowSet,
        mutation::{Mutation as MutationKind, SetCell},
        read_rows_response::{CellChunk, cell_chunk::RowStatus},
    },
};
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::{
    ColumnFamily, CreateTableRequest, DeleteTableRequest, Table,
    bigtable_table_admin_client::BigtableTableAdminClient,
};
#[cfg(feature = "opentelemetry")]
use opentelemetry::metrics::MeterProvider as _;
#[cfg(feature = "opentelemetry")]
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    data::{ResourceMetrics, ScopeMetrics},
};
use serde::Deserialize;
use tonic::{Request, metadata::MetadataValue, transport::Endpoint};

const PROJECT_ID: &str = "test-project";
const INSTANCE_ID: &str = "test-instance";
const FAMILY: &str = "family";
const QUALIFIER: &[u8] = b"qualifier";

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct Preferences {
    theme: String,
}

#[derive(Debug, Eq, PartialEq, bigtable_client::FromRow)]
#[bigtable(family = "family")]
struct TypedUser {
    #[bigtable(row_key)]
    key: String,
    name: String,
    age: u32,
    nickname: Option<String>,
    #[bigtable(json)]
    preferences: Preferences,
}

struct Observability {
    diagnostics: Arc<Mutex<Vec<DiagnosticEvent>>>,
    #[cfg(feature = "opentelemetry")]
    exporter: InMemoryMetricExporter,
    #[cfg(feature = "opentelemetry")]
    provider: SdkMeterProvider,
}

impl Observability {
    fn assert_finished(self) {
        let diagnostics = self.diagnostics.lock().expect("diagnostics lock");
        let operation_starts = diagnostics
            .iter()
            .filter(|event| matches!(event, DiagnosticEvent::OperationStarted { .. }))
            .count();
        let operation_finishes = diagnostics
            .iter()
            .filter(|event| matches!(event, DiagnosticEvent::OperationFinished { .. }))
            .count();
        let attempt_starts = diagnostics
            .iter()
            .filter(|event| matches!(event, DiagnosticEvent::AttemptStarted { .. }))
            .count();
        let attempt_finishes = diagnostics
            .iter()
            .filter(|event| matches!(event, DiagnosticEvent::AttemptFinished { .. }))
            .count();
        assert!(operation_starts >= 10);
        assert_eq!(operation_starts, operation_finishes);
        assert_eq!(attempt_starts, attempt_finishes);
        assert!(diagnostics.iter().any(|event| matches!(
            event,
            DiagnosticEvent::OperationStarted {
                operation: BigtableOperation::ReadRows,
                ..
            }
        )));
        assert!(diagnostics.iter().any(|event| matches!(
            event,
            DiagnosticEvent::OperationStarted {
                operation: BigtableOperation::MutateRows,
                ..
            }
        )));
        assert!(diagnostics.iter().all(|event| !matches!(
            event,
            DiagnosticEvent::OperationFinished { code, .. } if *code != tonic::Code::Ok
        )));
        drop(diagnostics);

        #[cfg(feature = "opentelemetry")]
        self.assert_metrics();
    }

    #[cfg(feature = "opentelemetry")]
    fn assert_metrics(self) {
        self.provider.force_flush().expect("flush emulator metrics");
        let metric_names = self
            .exporter
            .get_finished_metrics()
            .expect("finished emulator metrics")
            .iter()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(ScopeMetrics::metrics)
            .map(|metric| metric.name().to_owned())
            .collect::<HashSet<_>>();
        assert!(metric_names.contains("bigtable.googleapis.com/client/operation_latencies"));
        assert!(metric_names.contains("bigtable.googleapis.com/client/attempt_latencies"));
        assert!(metric_names.contains("bigtable.googleapis.com/client/first_response_latencies"));
        assert!(
            metric_names.contains("bigtable.googleapis.com/client/application_blocking_latencies")
        );
        self.provider
            .shutdown()
            .expect("shut down emulator metrics");
    }
}

async fn connect_client(endpoint: String) -> (Client, Observability) {
    let diagnostics = Arc::new(Mutex::new(Vec::new()));
    let observer_diagnostics = Arc::clone(&diagnostics);
    #[cfg(feature = "opentelemetry")]
    let exporter = InMemoryMetricExporter::default();
    #[cfg(feature = "opentelemetry")]
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    let config = ClientConfig::new(PROJECT_ID, INSTANCE_ID)
        .expect("valid config")
        .with_emulator_host(endpoint)
        .expect("valid emulator")
        .with_channel_pool_size(2)
        .expect("valid channel pool");
    let builder =
        Client::builder(config).with_diagnostic_observer(move |event: &DiagnosticEvent| {
            observer_diagnostics
                .lock()
                .expect("diagnostics lock")
                .push(event.clone());
        });
    #[cfg(feature = "opentelemetry")]
    let builder = builder.with_meter(provider.meter("emulator-test"));
    let client = builder
        .connect()
        .await
        .expect("client connects without credentials");
    assert!(client.config().uses_emulator());
    assert_eq!(client.config().channel_pool_size(), 2);

    (
        client,
        Observability {
            diagnostics,
            #[cfg(feature = "opentelemetry")]
            exporter,
            #[cfg(feature = "opentelemetry")]
            provider,
        },
    )
}

#[tokio::test]
async fn raw_and_high_level_clients_cover_reads_and_bulk_mutations() {
    if env::var_os("RUN_BIGTABLE_EMULATOR_TESTS").is_none() {
        return;
    }

    let host = env::var("BIGTABLE_EMULATOR_HOST").expect("emulator host is required");
    let endpoint = if host.contains("://") {
        host
    } else {
        format!("http://{host}")
    };
    let parent = format!("projects/{PROJECT_ID}/instances/{INSTANCE_ID}");
    let table_id = unique_table_id();
    let table_name = format!("{parent}/tables/{table_id}");
    let mut admin = connect_admin(&endpoint).await;
    create_table(&mut admin, parent, &table_id).await;

    let (client, observability) = connect_client(endpoint).await;

    let mut raw = client.raw_client();
    write_raw_row(&mut raw, &table_name, b"row-raw", b"raw value").await;

    let result = write_bulk_fixture(&client, &table_id).await;
    client
        .mutate_row(
            &table_id,
            set_row(b"row-single", b"single value").expect("valid single row"),
        )
        .await
        .expect("single-row mutation succeeds");
    write_typed_fixture(&client, &table_id).await;

    let raw_chunks = raw_read(&mut raw, &table_name, b"row-raw").await;
    let row_one = read_required(&client, &table_id, b"row-1").await;
    let row_two = read_required(&client, &table_id, b"row-2").await;
    let range_row = read_required(&client, &table_id, b"row-range").await;
    let deleted_row = client
        .read_row(&table_id, b"row-delete".to_vec())
        .await
        .expect("deleted row read succeeds");
    let typed_point = client
        .read_row_as::<TypedUser>(&table_id, b"typed-z".to_vec())
        .await
        .expect("typed point read succeeds")
        .expect("typed row exists");
    let typed_missing = client
        .read_row_as::<TypedUser>(&table_id, b"typed-missing".to_vec())
        .await
        .expect("missing typed point read succeeds");
    let typed_results = read_typed_fixture(&client, &table_id).await;

    delete_bulk_fixture(&client, &table_id).await;
    let row_one_after_delete = client
        .read_row(&table_id, b"row-1".to_vec())
        .await
        .expect("deleted cell read succeeds");
    let row_two_after_delete = client
        .read_row(&table_id, b"row-2".to_vec())
        .await
        .expect("deleted family read succeeds");
    let scan_keys = scan_keys(&client, &table_id).await;

    admin
        .delete_table(DeleteTableRequest {
            name: table_name.clone(),
        })
        .await
        .expect("table is deleted");

    assert_eq!(result.entries(), 4);
    assert_eq!(result.request_batches(), 4);
    assert_eq!(result.rpc_attempts(), 4);
    assert_raw_row(&raw_chunks, b"row-raw", b"raw value");
    assert_eq!(cell_value(&row_one), b"first value");
    assert_eq!(cell_value(&row_two), b"\x00\xffsecond");
    assert_eq!(cell_value(&range_row), b"new value");
    assert!(deleted_row.is_none());
    assert!(row_one_after_delete.is_none());
    assert!(row_two_after_delete.is_none());
    assert_eq!(
        typed_point,
        TypedUser {
            key: "typed-z".to_owned(),
            name: "Zoe".to_owned(),
            age: 42,
            nickname: None,
            preferences: Preferences {
                theme: "dark".to_owned(),
            },
        }
    );
    assert!(typed_missing.is_none());
    assert_typed_results(&typed_results);
    assert_eq!(
        scan_keys,
        vec![
            b"row-single".to_vec(),
            b"row-raw".to_vec(),
            b"row-range".to_vec(),
        ]
    );

    observability.assert_finished();
}

async fn write_typed_fixture(client: &Client, table_id: &str) {
    let bulk = BulkMutation::new(table_id)
        .expect("valid table")
        .entry(typed_row(b"typed-z", b"Zoe", b"42"))
        .expect("nonempty row")
        .entry(typed_row(b"typed-m", b"Mal", b"many"))
        .expect("nonempty row")
        .entry(typed_row(b"typed-a", b"Ada", b"37"))
        .expect("nonempty row");

    let result = client
        .mutate_rows(bulk)
        .await
        .expect("typed fixture writes succeed");
    assert_eq!(result.entries(), 3);
}

fn typed_row(row_key: &[u8], name: &[u8], age: &[u8]) -> RowMutation {
    RowMutation::new(row_key.to_vec())
        .expect("valid row")
        .mutation(Mutation::set_cell(FAMILY, b"name".to_vec(), name.to_vec()).expect("valid name"))
        .expect("within limit")
        .mutation(Mutation::set_cell(FAMILY, b"age".to_vec(), age.to_vec()).expect("valid age"))
        .expect("within limit")
        .mutation(
            Mutation::set_cell(
                FAMILY,
                b"preferences".to_vec(),
                br#"{"theme":"dark"}"#.to_vec(),
            )
            .expect("valid preferences"),
        )
        .expect("within limit")
}

async fn read_typed_fixture(client: &Client, table_id: &str) -> Vec<Result<TypedUser, Error>> {
    let query = Query::new(table_id)
        .expect("valid table")
        .prefix(b"typed-".to_vec())
        .reversed();
    let mut rows = client
        .read_rows_as_with_options(query, ReadOptions::default())
        .await
        .expect("typed scan starts");
    let mut results = Vec::new();
    while let Some(row) = rows.next().await {
        results.push(row);
    }
    results
}

fn assert_typed_results(results: &[Result<TypedUser, Error>]) {
    assert_eq!(results.len(), 3);
    assert!(matches!(
        &results[0],
        Ok(user) if user.key == "typed-z" && user.age == 42
    ));
    assert!(matches!(
        &results[1],
        Err(Error::RowMapping(error))
            if error.row_key().as_ref() == b"typed-m"
                && matches!(error.issue(), RowMappingIssue::InvalidValue { .. })
    ));
    assert!(matches!(
        &results[2],
        Ok(user) if user.key == "typed-a" && user.age == 37
    ));
}

async fn connect_admin(endpoint: &str) -> BigtableTableAdminClient<tonic::transport::Channel> {
    let channel = Endpoint::from_shared(endpoint.to_owned())
        .expect("valid emulator endpoint")
        .connect()
        .await
        .expect("emulator is reachable");
    BigtableTableAdminClient::new(channel)
}

async fn create_table(
    admin: &mut BigtableTableAdminClient<tonic::transport::Channel>,
    parent: String,
    table_id: &str,
) {
    admin
        .create_table(CreateTableRequest {
            parent,
            table_id: table_id.to_owned(),
            table: Some(Table {
                column_families: HashMap::from([(FAMILY.to_owned(), ColumnFamily::default())]),
                ..Table::default()
            }),
            initial_splits: Vec::new(),
        })
        .await
        .expect("table is created");
}

async fn write_bulk_fixture(
    client: &Client,
    table_id: &str,
) -> bigtable_client::BulkMutationResult {
    let range_row = RowMutation::new(b"row-range".to_vec())
        .expect("valid row")
        .mutation(
            Mutation::set_cell_at(FAMILY, QUALIFIER, 1_000, b"old value".to_vec())
                .expect("valid old cell"),
        )
        .expect("within limit")
        .mutation(
            Mutation::set_cell_at(FAMILY, QUALIFIER, 2_000, b"new value".to_vec())
                .expect("valid new cell"),
        )
        .expect("within limit")
        .mutation(
            Mutation::delete_cells_in_range(FAMILY, QUALIFIER, 0, 2_000)
                .expect("valid timestamp range"),
        )
        .expect("within limit");
    let deleted_row = set_row(b"row-delete", b"temporary")
        .expect("valid row")
        .mutation(Mutation::delete_row())
        .expect("within limit");
    let bulk = BulkMutation::new(table_id)
        .expect("valid table")
        .entry(set_row(b"row-1", b"first value").expect("valid row"))
        .expect("nonempty row")
        .entry(set_row(b"row-2", b"\x00\xffsecond").expect("valid row"))
        .expect("nonempty row")
        .entry(deleted_row)
        .expect("nonempty row")
        .entry(range_row)
        .expect("nonempty row");
    let options = BulkMutationOptions {
        batch: BatchPolicy {
            max_entries_per_request: 1,
            max_request_bytes: 1024,
            max_in_flight_requests: 2,
        },
        ..BulkMutationOptions::default()
    };

    client
        .mutate_rows_with_options(bulk, options)
        .await
        .expect("bulk mutation succeeds")
}

async fn delete_bulk_fixture(client: &Client, table_id: &str) {
    let row_one = RowMutation::new(b"row-1".to_vec())
        .expect("valid row")
        .mutation(Mutation::delete_cells(FAMILY, QUALIFIER).expect("valid cell delete"))
        .expect("within limit");
    let row_two = RowMutation::new(b"row-2".to_vec())
        .expect("valid row")
        .mutation(Mutation::delete_family(FAMILY).expect("valid family delete"))
        .expect("within limit");
    let bulk = BulkMutation::new(table_id)
        .expect("valid table")
        .entry(row_one)
        .expect("nonempty row")
        .entry(row_two)
        .expect("nonempty row");

    let result = client
        .mutate_rows(bulk)
        .await
        .expect("bulk deletes succeed");
    assert_eq!(result.entries(), 2);
}

fn set_row(row_key: &[u8], value: &[u8]) -> Result<RowMutation, bigtable_client::Error> {
    RowMutation::new(row_key.to_vec())?.mutation(Mutation::set_cell(
        FAMILY,
        QUALIFIER,
        value.to_vec(),
    )?)
}

async fn read_required(client: &Client, table_id: &str, row_key: &[u8]) -> Row {
    client
        .read_row(table_id, row_key.to_vec())
        .await
        .expect("point read succeeds")
        .expect("row exists")
}

async fn scan_keys(client: &Client, table_id: &str) -> Vec<Vec<u8>> {
    let query = Query::new(table_id)
        .expect("valid table")
        .prefix(b"row-".to_vec())
        .reversed();
    let mut rows = client.read_rows(query).await.expect("scan starts");
    let mut keys = Vec::new();
    while let Some(row) = rows.next().await {
        keys.push(row.expect("valid row").key.to_vec());
    }
    keys
}

async fn write_raw_row(raw: &mut RawClient, table_name: &str, key: &[u8], value: &[u8]) {
    let mutation = MutateRowRequest {
        table_name: table_name.to_owned(),
        app_profile_id: "default".to_owned(),
        row_key: key.to_vec().into(),
        mutations: vec![ProtoMutation {
            mutation: Some(MutationKind::SetCell(SetCell {
                family_name: FAMILY.to_owned(),
                column_qualifier: QUALIFIER.into(),
                timestamp_micros: -1,
                value: value.to_vec().into(),
            })),
        }],
        ..MutateRowRequest::default()
    };
    raw.mutate_row(routed_request(mutation, table_name))
        .await
        .expect("raw row mutation succeeds");
}

async fn raw_read(raw: &mut RawClient, table_name: &str, row_key: &[u8]) -> Vec<CellChunk> {
    let read = ReadRowsRequest {
        table_name: table_name.to_owned(),
        app_profile_id: "default".to_owned(),
        rows: Some(RowSet {
            row_keys: vec![row_key.to_vec().into()],
            row_ranges: Vec::new(),
        }),
        rows_limit: 1,
        ..ReadRowsRequest::default()
    };
    let mut stream = raw
        .read_rows(routed_request(read, table_name))
        .await
        .expect("read starts")
        .into_inner();
    let mut chunks = Vec::new();
    while let Some(response) = stream.message().await.expect("stream stays healthy") {
        chunks.extend(response.chunks);
    }
    chunks
}

fn cell_value(row: &Row) -> &[u8] {
    row.families[0].columns[0].cells[0].value.as_ref()
}

fn assert_raw_row(chunks: &[CellChunk], row_key: &[u8], value: &[u8]) {
    assert!(!chunks.is_empty());
    assert_eq!(
        chunks.first().expect("first chunk").row_key.as_ref(),
        row_key
    );
    assert_eq!(chunks.first().expect("first chunk").value.as_ref(), value);
    assert!(
        chunks
            .iter()
            .any(|chunk| matches!(chunk.row_status, Some(RowStatus::CommitRow(true))))
    );
}

fn routed_request<T>(message: T, table_name: &str) -> Request<T> {
    let mut request = Request::new(message);
    let routing = format!("table_name={table_name}")
        .parse::<MetadataValue<_>>()
        .expect("valid routing metadata");
    request
        .metadata_mut()
        .insert("x-goog-request-params", routing);
    request
}

fn unique_table_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos();
    format!("m2_{}_{}", process::id(), nanos)
}
