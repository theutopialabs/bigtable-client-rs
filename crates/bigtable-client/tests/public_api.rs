//! Public API contract tests.

use std::time::Duration;

use bigtable_client::{
    BulkMutation, Cell, ClientConfig, Column, ConfigField, ConfigIssue, Error, Family, FromRow,
    Mutation, MutationIssue, Query, QueryIssue, Row, RowDecoder, RowMappingIssue, RowMutation,
    RowStream, TypedRowStream,
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
    r#type: String,
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
        .with_app_profile_id(" analytics ")
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
fn public_validation_errors_are_typed() {
    let config_error = ClientConfig::new("project", "instance")
        .expect("valid IDs")
        .with_app_profile_id("a".repeat(51))
        .expect_err("oversized app profile");
    assert!(matches!(
        config_error,
        Error::InvalidConfig {
            field: ConfigField::AppProfileId,
            issue: ConfigIssue::TooLong { max_chars: 50 },
        }
    ));

    let mutation_error =
        Mutation::delete_family("a".repeat(65)).expect_err("oversized column family");
    assert!(matches!(
        mutation_error,
        Error::InvalidMutation {
            issue: MutationIssue::FamilyNameTooLong,
        }
    ));
    let query_error = Query::new("").expect_err("table ID is required");
    assert!(matches!(
        query_error,
        Error::InvalidQuery {
            issue: QueryIssue::EmptyTableId,
        }
    ));
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
                    column(b"type", b"person"),
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
            r#type: "person".to_owned(),
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
