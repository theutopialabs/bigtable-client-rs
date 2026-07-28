//! Public API contract tests.

use std::{sync::Arc, time::Duration};

use bigtable_client::{
    BatchPolicy, BigtableOperation, BulkMutation, BulkMutationOptions, BulkMutationPolicyIssue,
    Cell, Client, ClientConfig, Column, ConfigField, ConfigIssue, DeadlinePolicy, DiagnosticEvent,
    DiagnosticObserver, Error, Family, FromRow, Jitter, Mutation, MutationIssue, Query, QueryIssue,
    ReadOptions, RetryPolicy, Row, RowBound, RowDecoder, RowMappingIssue, RowMutation, RowRange,
    RowStream, TypedRowStream, proto,
};
use bytes::Bytes;
use futures_core::Stream;
use serde::Deserialize;

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct Settings {
    theme: String,
}

#[derive(Debug, Eq, PartialEq, bigtable_client::FromRow)]
#[bigtable(family = "profile")]
struct UserRecord {
    #[bigtable(row_key)]
    key: String,
    name: String,
    #[bigtable(qualifier = "is_active")]
    active: bool,
    nickname: Option<String>,
    #[bigtable(default)]
    visits: u64,
    #[bigtable(json)]
    settings: Settings,
    #[bigtable(family = "metrics", qualifier = "score", with = "decode_hex")]
    score: u16,
    #[bigtable(family = "binary", qualifier = b"\xff")]
    payload: Bytes,
}

#[derive(Debug, Eq, PartialEq, bigtable_client::FromRow)]
#[bigtable(family = "data")]
struct GenericRecord<T> {
    #[bigtable(row_key)]
    key: Bytes,
    value: T,
}

fn decode_hex(value: &[u8]) -> Result<u16, std::num::ParseIntError> {
    u16::from_str_radix(std::str::from_utf8(value).unwrap_or(""), 16)
}

#[test]
fn public_config_api_builds_a_valid_emulator_target() {
    let config = ClientConfig::new("project", "instance")
        .expect("valid IDs")
        .with_app_profile_id("analytics")
        .expect("valid app profile")
        .with_emulator_host("localhost:8086")
        .expect("valid emulator")
        .with_channel_pool_size(2)
        .expect("valid pool")
        .with_connect_timeout(Duration::from_secs(2))
        .expect("valid timeout")
        .with_request_timeout(Duration::from_secs(3))
        .expect("valid timeout")
        .with_keep_alive(Duration::from_secs(4), Duration::from_secs(5))
        .expect("valid keepalive");

    assert_eq!(config.project_id(), "project");
    assert_eq!(config.instance_id(), "instance");
    assert_eq!(config.app_profile_id(), "analytics");
    assert_eq!(config.service_endpoint(), "http://localhost:8086");
    assert!(config.uses_emulator());
}

#[test]
fn public_errors_can_be_matched_without_string_parsing() {
    let error = ClientConfig::new("", "instance").expect_err("project ID is required");

    assert!(matches!(
        error,
        Error::InvalidConfig {
            field: ConfigField::ProjectId,
            issue: ConfigIssue::Empty,
        }
    ));
}

#[test]
fn generated_proto_types_are_available() {
    let request = proto::ReadRowsRequest {
        table_name: "projects/p/instances/i/tables/t".to_owned(),
        ..proto::ReadRowsRequest::default()
    };

    assert_eq!(request.table_name, "projects/p/instances/i/tables/t");
}

#[test]
fn public_query_and_row_types_support_binary_data() {
    let query = Query::new("events")
        .expect("valid table")
        .row_key(Bytes::from_static(b"\x00one"))
        .row_range(RowRange::new(
            RowBound::inclusive(Bytes::from_static(b"a")),
            RowBound::exclusive(Bytes::from_static(b"z")),
        ))
        .prefix(Bytes::from_static(b"user#"))
        .limit(10)
        .expect("valid limit")
        .reversed();
    let row = Row {
        key: Bytes::from_static(b"\x00one"),
        families: vec![Family {
            name: "data".to_owned(),
            columns: vec![Column {
                qualifier: Bytes::from_static(b"\xffpayload"),
                cells: vec![Cell {
                    timestamp_micros: 42,
                    value: Bytes::from_static(b"\x00\xff"),
                    labels: vec!["match".to_owned()],
                }],
            }],
        }],
    };

    assert!(format!("{query:?}").contains("events"));
    assert_eq!(row.key.as_ref(), b"\x00one");
    assert_eq!(
        row.families[0].columns[0].cells[0].value.as_ref(),
        b"\x00\xff"
    );
}

#[test]
fn public_read_policies_are_configurable() {
    let options = ReadOptions {
        retry: RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(25),
            max_backoff: Duration::from_secs(2),
            multiplier: 1.5,
            jitter: Jitter::None,
        },
        deadlines: DeadlinePolicy {
            operation_timeout: Duration::from_secs(30),
            attempt_timeout: Duration::from_secs(5),
        },
    };

    assert_eq!(options.retry.max_attempts, 5);
    assert_eq!(options.deadlines.attempt_timeout, Duration::from_secs(5));
}

#[test]
fn public_observability_api_builds_without_connecting() {
    let config = ClientConfig::new("project", "instance").expect("valid config");
    let builder =
        Client::builder(config.clone()).with_diagnostic_observer(|_event: &DiagnosticEvent| {});
    assert!(format!("{builder:?}").contains("has_diagnostic_observer: true"));

    let observer: Arc<dyn DiagnosticObserver> = Arc::new(|_event: &DiagnosticEvent| {});
    let shared = Client::builder(config).with_shared_diagnostic_observer(observer);
    assert!(format!("{shared:?}").contains("has_diagnostic_observer: true"));

    let operation = BigtableOperation::ReadRows;
    assert!(matches!(operation, BigtableOperation::ReadRows));
}

#[cfg(feature = "opentelemetry")]
#[test]
fn public_builder_accepts_a_custom_meter() {
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    let provider = SdkMeterProvider::default();
    let meter = provider.meter("public-api-test");
    let builder = Client::builder(ClientConfig::new("project", "instance").expect("valid config"))
        .with_meter(meter);

    assert!(format!("{builder:?}").contains("has_custom_meter: true"));
}

#[test]
fn public_mutation_types_support_binary_data_and_retry_safety() {
    let row = RowMutation::new(Bytes::from_static(b"\x00row"))
        .expect("valid binary row key")
        .mutation(
            Mutation::set_cell_at(
                "data",
                Bytes::from_static(b"\xffqualifier"),
                1_000,
                Bytes::from_static(b"\x00\xff"),
            )
            .expect("valid cell"),
        )
        .expect("within mutation limit")
        .mutation(Mutation::delete_family("old_data").expect("valid family"))
        .expect("within mutation limit");
    let bulk = BulkMutation::new("events")
        .expect("valid table")
        .entry(row.clone())
        .expect("nonempty row");

    assert_eq!(row.row_key().as_ref(), b"\x00row");
    assert_eq!(row.len(), 2);
    assert!(row.is_retry_safe());
    assert_eq!(bulk.table_id(), "events");
    assert_eq!(bulk.len(), 1);

    let unsafe_row = RowMutation::new("server-time")
        .expect("valid row")
        .mutation(
            Mutation::set_cell_at_server_time("data", "value", "payload")
                .expect("valid server-time cell"),
        )
        .expect("within mutation limit");
    assert!(!unsafe_row.is_retry_safe());
}

#[test]
fn public_derive_maps_sparse_json_custom_and_binary_fields() {
    let row = Row {
        key: Bytes::from_static(b"user#1"),
        families: vec![
            Family {
                name: "profile".to_owned(),
                columns: vec![
                    column(b"name", b"Ada"),
                    column(b"is_active", b"true"),
                    column(b"settings", br#"{"theme":"dark"}"#),
                ],
            },
            Family {
                name: "metrics".to_owned(),
                columns: vec![column(b"score", b"ff")],
            },
            Family {
                name: "binary".to_owned(),
                columns: vec![column(b"\xff", b"\x00\xff")],
            },
        ],
    };

    assert_eq!(
        UserRecord::from_row(row).expect("derived row"),
        UserRecord {
            key: "user#1".to_owned(),
            name: "Ada".to_owned(),
            active: true,
            nickname: None,
            visits: 0,
            settings: Settings {
                theme: "dark".to_owned(),
            },
            score: 255,
            payload: Bytes::from_static(b"\x00\xff"),
        }
    );
}

#[test]
fn public_derive_supports_generic_targets() {
    let row = Row {
        key: Bytes::from_static(b"generic"),
        families: vec![Family {
            name: "data".to_owned(),
            columns: vec![column(b"value", b"hello")],
        }],
    };

    assert_eq!(
        GenericRecord::<String>::from_row(row).expect("generic row"),
        GenericRecord {
            key: Bytes::from_static(b"generic"),
            value: "hello".to_owned(),
        }
    );
}

#[test]
fn public_row_decoder_errors_can_be_matched_without_string_parsing() {
    let row = Row {
        key: Bytes::from_static(b"user#1"),
        families: Vec::new(),
    };
    let error = RowDecoder::new(&row)
        .required::<String>("profile", b"name")
        .expect_err("family is required");

    assert!(matches!(
        error.issue(),
        RowMappingIssue::MissingFamily { family } if family == "profile"
    ));
    assert_eq!(error.row_key().as_ref(), b"user#1");
}

#[test]
fn public_mutation_errors_are_typed() {
    let empty_row = RowMutation::new("row").expect("valid row");
    let error = BulkMutation::new("events")
        .expect("valid table")
        .entry(empty_row)
        .expect_err("empty row mutation is rejected");

    assert!(matches!(
        error,
        Error::InvalidMutation {
            issue: MutationIssue::EmptyRowMutation
        }
    ));
}

#[test]
fn public_bulk_mutation_policies_are_configurable() {
    let options = BulkMutationOptions {
        retry: RetryPolicy {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_secs(1),
            multiplier: 2.0,
            jitter: Jitter::None,
        },
        deadlines: DeadlinePolicy {
            operation_timeout: Duration::from_secs(20),
            attempt_timeout: Duration::from_secs(3),
        },
        batch: BatchPolicy {
            max_entries_per_request: 50,
            max_request_bytes: 4 * 1024 * 1024,
            max_in_flight_requests: 3,
        },
    };

    assert_eq!(options.batch.max_entries_per_request, 50);
    assert_eq!(options.batch.max_in_flight_requests, 3);
    assert_eq!(
        BulkMutationPolicyIssue::ZeroInFlightRequests.to_string(),
        "max_in_flight_requests must be greater than zero"
    );
}

#[test]
fn public_query_errors_are_typed() {
    let error = Query::new("").expect_err("table ID is required");

    assert!(matches!(
        error,
        Error::InvalidQuery {
            issue: QueryIssue::EmptyTableId
        }
    ));
}

#[test]
fn row_stream_implements_the_standard_stream_trait() {
    fn assert_stream<T, I>()
    where
        T: Stream<Item = Result<I, Error>>,
    {
    }

    assert_stream::<RowStream, Row>();
    assert_stream::<TypedRowStream<UserRecord>, UserRecord>();
}

fn column(qualifier: &'static [u8], value: &'static [u8]) -> Column {
    Column {
        qualifier: Bytes::from_static(qualifier),
        cells: vec![Cell {
            timestamp_micros: 1_000,
            value: Bytes::from_static(value),
            labels: Vec::new(),
        }],
    }
}
