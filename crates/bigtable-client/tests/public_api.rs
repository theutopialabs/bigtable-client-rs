//! Public API contract tests.

use std::time::Duration;

use bigtable_client::{
    Cell, ClientConfig, Column, ConfigField, ConfigIssue, DeadlinePolicy, Error, Family, Jitter,
    Query, QueryIssue, ReadOptions, RetryPolicy, Row, RowBound, RowRange, RowStream, proto,
};
use bytes::Bytes;
use futures_core::Stream;

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
    fn assert_stream<T>()
    where
        T: Stream<Item = Result<Row, Error>>,
    {
    }

    assert_stream::<RowStream>();
}
