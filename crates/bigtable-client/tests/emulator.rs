//! Bigtable emulator tests.

#![cfg(feature = "emulator-tests")]

use std::{
    collections::HashMap,
    env, process,
    time::{Duration, SystemTime},
};

use bigtable_client::{
    Client, ClientConfig, Query, RawClient, Row,
    proto::{
        MutateRowRequest, Mutation, ReadRowsRequest, RowSet,
        mutation::{Mutation as MutationKind, SetCell},
        read_rows_response::{CellChunk, cell_chunk::RowStatus},
    },
};
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::{
    ColumnFamily, CreateTableRequest, DeleteTableRequest, Table,
    bigtable_table_admin_client::BigtableTableAdminClient,
};
use tonic::{Request, metadata::MetadataValue, transport::Endpoint};

const PROJECT_ID: &str = "test-project";
const INSTANCE_ID: &str = "test-instance";
const FAMILY: &str = "family";

#[tokio::test]
async fn raw_client_creates_mutates_and_reads_over_emulator() {
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
    let row_key = b"row-1";
    let value = b"hello from M1";
    let second_row_key = b"row-2";
    let second_value = b"second row";

    let channel = Endpoint::from_shared(endpoint.clone())
        .expect("valid emulator endpoint")
        .connect()
        .await
        .expect("emulator is reachable");
    let mut admin = BigtableTableAdminClient::new(channel);
    admin
        .create_table(CreateTableRequest {
            parent,
            table_id: table_id.clone(),
            table: Some(Table {
                column_families: HashMap::from([(FAMILY.to_owned(), ColumnFamily::default())]),
                ..Table::default()
            }),
            initial_splits: Vec::new(),
        })
        .await
        .expect("table is created");

    let config = ClientConfig::new(PROJECT_ID, INSTANCE_ID)
        .expect("valid config")
        .with_emulator_host(endpoint)
        .expect("valid emulator")
        .with_channel_pool_size(2)
        .expect("valid channel pool");
    let client = Client::connect(config)
        .await
        .expect("client connects without credentials");
    assert!(client.config().uses_emulator());
    assert_eq!(client.config().channel_pool_size(), 2);

    let mut raw = client.raw_client();
    for (key, cell_value) in [
        (row_key.as_slice(), value.as_slice()),
        (second_row_key.as_slice(), second_value.as_slice()),
    ] {
        write_row(&mut raw, &table_name, key, cell_value).await;
    }

    let chunks = raw_read(&mut raw, &table_name, row_key).await;
    let (exact, query_rows) = high_level_reads(&client, &table_id, row_key).await;

    admin
        .delete_table(DeleteTableRequest {
            name: table_name.clone(),
        })
        .await
        .expect("table is deleted");

    assert_results(&chunks, &exact, &query_rows, row_key, value, second_row_key);
}

async fn write_row(raw: &mut RawClient, table_name: &str, key: &[u8], value: &[u8]) {
    let mutation = MutateRowRequest {
        table_name: table_name.to_owned(),
        app_profile_id: "default".to_owned(),
        row_key: key.to_vec().into(),
        mutations: vec![Mutation {
            mutation: Some(MutationKind::SetCell(SetCell {
                family_name: FAMILY.to_owned(),
                column_qualifier: b"qualifier".as_slice().into(),
                timestamp_micros: -1,
                value: value.to_vec().into(),
            })),
        }],
        ..MutateRowRequest::default()
    };
    raw.mutate_row(routed_request(mutation, table_name))
        .await
        .expect("row mutation succeeds");
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

async fn high_level_reads(client: &Client, table_id: &str, row_key: &[u8]) -> (Row, Vec<Row>) {
    let exact = client
        .read_row(table_id, row_key.to_vec())
        .await
        .expect("high-level point read succeeds")
        .expect("row exists");
    let query = Query::new(table_id)
        .expect("valid table")
        .prefix(b"row-".to_vec())
        .limit(2)
        .expect("valid limit")
        .reversed();
    let mut stream = client
        .read_rows(query)
        .await
        .expect("high-level scan starts");
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.expect("high-level row is valid"));
    }
    (exact, rows)
}

fn assert_results(
    chunks: &[CellChunk],
    exact: &Row,
    query_rows: &[Row],
    row_key: &[u8],
    value: &[u8],
    second_row_key: &[u8],
) {
    assert!(!chunks.is_empty());
    assert_eq!(
        chunks.first().expect("first chunk").row_key.as_ref(),
        row_key
    );
    assert_eq!(chunks.first().expect("first chunk").value.as_ref(), value);
    assert!(
        chunks
            .iter()
            .any(|chunk| { matches!(chunk.row_status, Some(RowStatus::CommitRow(true))) })
    );
    assert_eq!(exact.key.as_ref(), row_key);
    assert_eq!(exact.families[0].columns[0].cells[0].value.as_ref(), value);
    assert_eq!(
        query_rows
            .iter()
            .map(|row| row.key.as_ref())
            .collect::<Vec<_>>(),
        vec![second_row_key, row_key]
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
    format!("m1_{}_{}", process::id(), nanos)
}
