use std::fmt;

use thiserror::Error;

/// A client operation error.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The client configuration contains an invalid value.
    #[error("invalid Bigtable configuration for {field}: {issue}")]
    InvalidConfig {
        /// The invalid field.
        field: ConfigField,
        /// Why the value is invalid.
        issue: ConfigIssue,
    },

    /// The environment configuration could not be loaded.
    #[error("failed to load Bigtable configuration: {source}")]
    ConfigLoad {
        /// The configuration source error.
        #[source]
        source: Box<figment::Error>,
    },

    /// Application default credentials could not provide an access token.
    #[error("failed to authenticate with Google Cloud: {source}")]
    Authentication {
        /// The authentication source error.
        #[source]
        source: Box<gcp_auth::Error>,
    },

    /// A Google Cloud access token could not be used as gRPC metadata.
    #[error("Google Cloud returned an invalid access token: {source}")]
    InvalidAccessToken {
        /// The metadata parsing error.
        #[source]
        source: tonic::metadata::errors::InvalidMetadataValue,
    },

    /// Static client metadata could not be encoded for a gRPC request.
    #[error("failed to encode the {header} metadata header: {source}")]
    InvalidClientMetadata {
        /// The header that failed.
        header: &'static str,
        /// The metadata parsing error.
        #[source]
        source: tonic::metadata::errors::InvalidMetadataValue,
    },

    /// The gRPC channel could not be built or connected.
    #[error("failed to connect to Bigtable: {source}")]
    Transport {
        /// The transport source error.
        #[source]
        source: Box<tonic::transport::Error>,
    },

    /// The internal channel pool stopped while it was being initialized.
    #[error("failed to initialize the Bigtable channel pool")]
    ChannelPoolClosed,

    /// A high-level read query contains an invalid value.
    #[error("invalid Bigtable query: {issue}")]
    InvalidQuery {
        /// Why the query is invalid.
        issue: QueryIssue,
    },

    /// Read retry or deadline settings contain an invalid value.
    #[error("invalid Bigtable read policy: {issue}")]
    InvalidReadPolicy {
        /// Why the policy is invalid.
        issue: ReadPolicyIssue,
    },

    /// A streamed `ReadRows` response violated the Bigtable wire contract.
    #[error("invalid Bigtable ReadRows response: {issue}")]
    InvalidReadRowsResponse {
        /// The invalid chunk or stream state.
        issue: RowMergeIssue,
    },

    /// A `ReadRows` RPC failed and could not be retried.
    #[error("Bigtable ReadRows failed after {attempts} attempt(s): {source}")]
    ReadRows {
        /// Attempts made for this operation.
        attempts: u32,
        /// The final gRPC status.
        #[source]
        source: tonic::Status,
    },

    /// The total `ReadRows` operation deadline was exhausted.
    #[error("Bigtable ReadRows exceeded its {timeout:?} deadline after {attempts} attempt(s)")]
    ReadDeadlineExceeded {
        /// Attempts made for this operation.
        attempts: u32,
        /// The configured operation timeout.
        timeout: std::time::Duration,
    },
}

/// The reason a streamed row could not be assembled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RowMergeIssue {
    /// A reset appeared before a row started.
    ResetBetweenRows,
    /// A new row did not include a row key.
    MissingRowKey,
    /// A new row did not include a family.
    MissingFamily,
    /// A new row did not include a qualifier.
    MissingQualifier,
    /// A row key changed before the current row committed.
    RowKeyChanged,
    /// A family changed without a qualifier.
    FamilyWithoutQualifier,
    /// A row or scan marker did not follow query order.
    OutOfOrderRowKey,
    /// A scan marker appeared while a row was incomplete.
    ScanMarkerDuringRow,
    /// A reset chunk included cell data.
    ResetWithData,
    /// A cell value size was negative.
    NegativeValueSize,
    /// A split cell started without any value bytes.
    SplitValueMissingData,
    /// A split cell received more bytes than declared.
    SplitValueTooLarge,
    /// A row committed before a split cell completed.
    CommitBeforeCellComplete,
    /// A split cell continuation repeated cell metadata.
    CellMetadataOnContinuation,
    /// A split cell changed its declared size.
    SplitValueSizeChanged,
    /// A split cell ended before reaching its declared size.
    SplitValueWrongSize,
    /// The response stream ended with an incomplete row.
    IncompleteRow,
}

impl fmt::Display for RowMergeIssue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ResetBetweenRows => "reset_row is not valid between rows",
            Self::MissingRowKey => "a new row is missing its row key",
            Self::MissingFamily => "a new row is missing its column family",
            Self::MissingQualifier => "a new row is missing its column qualifier",
            Self::RowKeyChanged => "the row key changed before commit_row",
            Self::FamilyWithoutQualifier => {
                "a new column family did not include a column qualifier"
            }
            Self::OutOfOrderRowKey => "row keys are not in strict query order",
            Self::ScanMarkerDuringRow => "last_scanned_row_key appeared during an incomplete row",
            Self::ResetWithData => "reset_row must not include cell data",
            Self::NegativeValueSize => "value_size must not be negative",
            Self::SplitValueMissingData => "a split cell must start with value bytes",
            Self::SplitValueTooLarge => "a split cell exceeded its declared value_size",
            Self::CommitBeforeCellComplete => "commit_row appeared before a split cell completed",
            Self::CellMetadataOnContinuation => "a split cell continuation repeated cell metadata",
            Self::SplitValueSizeChanged => {
                "a split cell continuation changed its declared value_size"
            }
            Self::SplitValueWrongSize => {
                "a split cell ended before reaching its declared value_size"
            }
            Self::IncompleteRow => "the response stream ended before commit_row",
        })
    }
}

impl Error {
    pub(crate) const fn invalid_config(field: ConfigField, issue: ConfigIssue) -> Self {
        Self::InvalidConfig { field, issue }
    }

    pub(crate) const fn invalid_query(issue: QueryIssue) -> Self {
        Self::InvalidQuery { issue }
    }

    pub(crate) const fn invalid_read_policy(issue: ReadPolicyIssue) -> Self {
        Self::InvalidReadPolicy { issue }
    }
}

/// The reason a high-level read query is invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QueryIssue {
    /// The table ID is empty.
    EmptyTableId,
    /// The table ID exceeds Bigtable's 50-character limit.
    TableIdTooLong,
    /// The table ID contains a resource path separator.
    TableIdContainsSlash,
    /// A row limit is zero.
    ZeroRowLimit,
    /// A row limit is larger than the API can represent.
    RowLimitTooLarge,
}

impl fmt::Display for QueryIssue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyTableId => "table_id must not be empty",
            Self::TableIdTooLong => "table_id must not exceed 50 characters",
            Self::TableIdContainsSlash => "table_id must not contain '/'",
            Self::ZeroRowLimit => "row limit must be greater than zero",
            Self::RowLimitTooLarge => "row limit must fit in a signed 64-bit integer",
        })
    }
}

/// The reason a read retry or deadline policy is invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReadPolicyIssue {
    /// No request attempts are allowed.
    ZeroMaxAttempts,
    /// The initial retry backoff is zero.
    ZeroInitialBackoff,
    /// The maximum retry backoff is zero.
    ZeroMaxBackoff,
    /// The maximum backoff is smaller than the initial backoff.
    MaxBackoffTooSmall,
    /// The backoff multiplier is below one or is not finite.
    InvalidBackoffMultiplier,
    /// The operation timeout is zero.
    ZeroOperationTimeout,
    /// The attempt timeout is zero.
    ZeroAttemptTimeout,
    /// A deadline cannot be represented by the runtime clock.
    DeadlineTooLarge,
}

impl fmt::Display for ReadPolicyIssue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroMaxAttempts => "max_attempts must be greater than zero",
            Self::ZeroInitialBackoff => "initial_backoff must be greater than zero",
            Self::ZeroMaxBackoff => "max_backoff must be greater than zero",
            Self::MaxBackoffTooSmall => "max_backoff must not be smaller than initial_backoff",
            Self::InvalidBackoffMultiplier => {
                "multiplier must be finite and greater than or equal to one"
            }
            Self::ZeroOperationTimeout => "operation_timeout must be greater than zero",
            Self::ZeroAttemptTimeout => "attempt_timeout must be greater than zero",
            Self::DeadlineTooLarge => "deadline is too large for the runtime clock",
        })
    }
}

/// A field in [`crate::ClientConfig`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigField {
    /// The Google Cloud project ID.
    ProjectId,
    /// The Bigtable instance ID.
    InstanceId,
    /// The Bigtable application profile ID.
    AppProfileId,
    /// The Bigtable service endpoint.
    Endpoint,
    /// The Bigtable emulator endpoint.
    EmulatorHost,
    /// The number of gRPC channels.
    ChannelPoolSize,
    /// The channel connection timeout.
    ConnectTimeout,
    /// The default high-level operation timeout.
    RequestTimeout,
    /// The HTTP/2 keepalive interval.
    KeepAliveInterval,
    /// The HTTP/2 keepalive timeout.
    KeepAliveTimeout,
}

impl fmt::Display for ConfigField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ProjectId => "project_id",
            Self::InstanceId => "instance_id",
            Self::AppProfileId => "app_profile_id",
            Self::Endpoint => "endpoint",
            Self::EmulatorHost => "emulator_host",
            Self::ChannelPoolSize => "channel_pool_size",
            Self::ConnectTimeout => "connect_timeout",
            Self::RequestTimeout => "request_timeout",
            Self::KeepAliveInterval => "keep_alive_interval",
            Self::KeepAliveTimeout => "keep_alive_timeout",
        })
    }
}

/// The reason a configuration value is invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigIssue {
    /// The value is empty.
    Empty,
    /// The value is not a valid absolute HTTP or HTTPS URI.
    InvalidUri,
    /// The number must be greater than zero.
    MustBePositive,
}

impl fmt::Display for ConfigIssue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "must not be empty",
            Self::InvalidUri => "must be an absolute HTTP or HTTPS URI",
            Self::MustBePositive => "must be greater than zero",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfigField, ConfigIssue, Error, QueryIssue, ReadPolicyIssue, RowMergeIssue};

    #[test]
    fn invalid_config_display_names_the_field_and_issue() {
        let error = Error::invalid_config(ConfigField::ProjectId, ConfigIssue::Empty);

        assert_eq!(
            error.to_string(),
            "invalid Bigtable configuration for project_id: must not be empty"
        );
    }

    #[test]
    fn config_load_display_includes_source() {
        let source = figment::Figment::new()
            .extract::<String>()
            .expect_err("empty config cannot produce a string");
        let source_message = source.to_string();
        let error = Error::ConfigLoad {
            source: Box::new(source),
        };

        assert_eq!(
            error.to_string(),
            format!("failed to load Bigtable configuration: {source_message}")
        );
    }

    #[test]
    fn invalid_access_token_display_includes_source() {
        let source = "bad\nvalue"
            .parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>()
            .expect_err("newlines are not valid metadata");
        let source_message = source.to_string();
        let error = Error::InvalidAccessToken { source };

        assert_eq!(
            error.to_string(),
            format!("Google Cloud returned an invalid access token: {source_message}")
        );
    }

    #[test]
    fn invalid_client_metadata_display_names_header_and_source() {
        let source = "bad\nvalue"
            .parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>()
            .expect_err("newlines are not valid metadata");
        let source_message = source.to_string();
        let error = Error::InvalidClientMetadata {
            header: "test-header",
            source,
        };

        assert_eq!(
            error.to_string(),
            format!("failed to encode the test-header metadata header: {source_message}")
        );
    }

    #[test]
    fn authentication_display_includes_source() {
        let error = Error::Authentication {
            source: Box::new(gcp_auth::Error::Str("test auth failure")),
        };

        assert_eq!(
            error.to_string(),
            "failed to authenticate with Google Cloud: test auth failure"
        );
    }

    #[test]
    fn channel_pool_error_has_recovery_context() {
        assert_eq!(
            Error::ChannelPoolClosed.to_string(),
            "failed to initialize the Bigtable channel pool"
        );
    }

    #[test]
    fn read_errors_include_attempt_and_recovery_context() {
        let rpc = Error::ReadRows {
            attempts: 3,
            source: tonic::Status::unavailable("try later"),
        };
        let deadline = Error::ReadDeadlineExceeded {
            attempts: 2,
            timeout: std::time::Duration::from_secs(5),
        };
        let wire = Error::InvalidReadRowsResponse {
            issue: RowMergeIssue::IncompleteRow,
        };

        let rpc_message = rpc.to_string();
        assert!(rpc_message.starts_with("Bigtable ReadRows failed after 3 attempt(s):"));
        assert!(rpc_message.contains("try later"));
        assert_eq!(
            deadline.to_string(),
            "Bigtable ReadRows exceeded its 5s deadline after 2 attempt(s)"
        );
        assert_eq!(
            wire.to_string(),
            "invalid Bigtable ReadRows response: the response stream ended before commit_row"
        );
    }

    #[test]
    fn every_config_field_has_a_stable_name() {
        let cases = [
            (ConfigField::ProjectId, "project_id"),
            (ConfigField::InstanceId, "instance_id"),
            (ConfigField::AppProfileId, "app_profile_id"),
            (ConfigField::Endpoint, "endpoint"),
            (ConfigField::EmulatorHost, "emulator_host"),
            (ConfigField::ChannelPoolSize, "channel_pool_size"),
            (ConfigField::ConnectTimeout, "connect_timeout"),
            (ConfigField::RequestTimeout, "request_timeout"),
            (ConfigField::KeepAliveInterval, "keep_alive_interval"),
            (ConfigField::KeepAliveTimeout, "keep_alive_timeout"),
        ];

        for (field, expected) in cases {
            assert_eq!(field.to_string(), expected);
        }
    }

    #[test]
    fn every_config_issue_has_clear_guidance() {
        let cases = [
            (ConfigIssue::Empty, "must not be empty"),
            (
                ConfigIssue::InvalidUri,
                "must be an absolute HTTP or HTTPS URI",
            ),
            (ConfigIssue::MustBePositive, "must be greater than zero"),
        ];

        for (issue, expected) in cases {
            assert_eq!(issue.to_string(), expected);
        }
    }

    #[test]
    fn every_query_issue_has_clear_guidance() {
        let cases = [
            (QueryIssue::EmptyTableId, "table_id must not be empty"),
            (
                QueryIssue::TableIdTooLong,
                "table_id must not exceed 50 characters",
            ),
            (
                QueryIssue::TableIdContainsSlash,
                "table_id must not contain '/'",
            ),
            (
                QueryIssue::ZeroRowLimit,
                "row limit must be greater than zero",
            ),
            (
                QueryIssue::RowLimitTooLarge,
                "row limit must fit in a signed 64-bit integer",
            ),
        ];

        for (issue, expected) in cases {
            assert_eq!(issue.to_string(), expected);
        }
    }

    #[test]
    fn every_read_policy_issue_has_clear_guidance() {
        let cases = [
            (
                ReadPolicyIssue::ZeroMaxAttempts,
                "max_attempts must be greater than zero",
            ),
            (
                ReadPolicyIssue::ZeroInitialBackoff,
                "initial_backoff must be greater than zero",
            ),
            (
                ReadPolicyIssue::ZeroMaxBackoff,
                "max_backoff must be greater than zero",
            ),
            (
                ReadPolicyIssue::MaxBackoffTooSmall,
                "max_backoff must not be smaller than initial_backoff",
            ),
            (
                ReadPolicyIssue::InvalidBackoffMultiplier,
                "multiplier must be finite and greater than or equal to one",
            ),
            (
                ReadPolicyIssue::ZeroOperationTimeout,
                "operation_timeout must be greater than zero",
            ),
            (
                ReadPolicyIssue::ZeroAttemptTimeout,
                "attempt_timeout must be greater than zero",
            ),
            (
                ReadPolicyIssue::DeadlineTooLarge,
                "deadline is too large for the runtime clock",
            ),
        ];

        for (issue, expected) in cases {
            assert_eq!(issue.to_string(), expected);
        }
    }

    #[test]
    fn every_row_merge_issue_has_clear_guidance() {
        let cases = [
            (
                RowMergeIssue::ResetBetweenRows,
                "reset_row is not valid between rows",
            ),
            (
                RowMergeIssue::MissingRowKey,
                "a new row is missing its row key",
            ),
            (
                RowMergeIssue::MissingFamily,
                "a new row is missing its column family",
            ),
            (
                RowMergeIssue::MissingQualifier,
                "a new row is missing its column qualifier",
            ),
            (
                RowMergeIssue::RowKeyChanged,
                "the row key changed before commit_row",
            ),
            (
                RowMergeIssue::FamilyWithoutQualifier,
                "a new column family did not include a column qualifier",
            ),
            (
                RowMergeIssue::OutOfOrderRowKey,
                "row keys are not in strict query order",
            ),
            (
                RowMergeIssue::ScanMarkerDuringRow,
                "last_scanned_row_key appeared during an incomplete row",
            ),
            (
                RowMergeIssue::ResetWithData,
                "reset_row must not include cell data",
            ),
            (
                RowMergeIssue::NegativeValueSize,
                "value_size must not be negative",
            ),
            (
                RowMergeIssue::SplitValueMissingData,
                "a split cell must start with value bytes",
            ),
            (
                RowMergeIssue::SplitValueTooLarge,
                "a split cell exceeded its declared value_size",
            ),
            (
                RowMergeIssue::CommitBeforeCellComplete,
                "commit_row appeared before a split cell completed",
            ),
            (
                RowMergeIssue::CellMetadataOnContinuation,
                "a split cell continuation repeated cell metadata",
            ),
            (
                RowMergeIssue::SplitValueSizeChanged,
                "a split cell continuation changed its declared value_size",
            ),
            (
                RowMergeIssue::SplitValueWrongSize,
                "a split cell ended before reaching its declared value_size",
            ),
            (
                RowMergeIssue::IncompleteRow,
                "the response stream ended before commit_row",
            ),
        ];

        for (issue, expected) in cases {
            assert_eq!(issue.to_string(), expected);
        }
    }
}
