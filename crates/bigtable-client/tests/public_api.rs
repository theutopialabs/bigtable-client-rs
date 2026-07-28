//! Public API contract tests.

use std::time::Duration;

use bigtable_client::{ClientConfig, ConfigField, ConfigIssue, Error, proto};

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
