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

    /// A high-level mutation contains a value Bigtable cannot accept.
    ///
    /// Check `issue`, fix the input, and rebuild the mutation before retrying.
    #[error("invalid Bigtable mutation: {issue}")]
    InvalidMutation {
        /// Why the mutation is invalid.
        issue: MutationIssue,
    },

    /// Bulk mutation retry, deadline, or batching settings are invalid.
    ///
    /// Check `issue`, fix the setting, and start the operation again.
    #[error("invalid Bigtable bulk mutation policy: {issue}")]
    InvalidBulkMutationPolicy {
        /// Why the policy is invalid.
        issue: BulkMutationPolicyIssue,
    },

    /// One or more row mutations did not receive a confirmed success.
    ///
    /// Inspect the grouped failure indexes before deciding what to replay.
    #[error(transparent)]
    BulkMutation(#[from] BulkMutationError),

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

    pub(crate) const fn invalid_mutation(issue: MutationIssue) -> Self {
        Self::InvalidMutation { issue }
    }

    pub(crate) const fn invalid_bulk_mutation_policy(issue: BulkMutationPolicyIssue) -> Self {
        Self::InvalidBulkMutationPolicy { issue }
    }

    pub(crate) const fn invalid_read_policy(issue: ReadPolicyIssue) -> Self {
        Self::InvalidReadPolicy { issue }
    }
}

/// Invalid bulk mutation retry, deadline, or batching settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BulkMutationPolicyIssue {
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
    /// No entries are allowed in a request.
    ZeroEntriesPerRequest,
    /// The target request byte size is zero.
    ZeroRequestBytes,
    /// No requests are allowed in flight.
    ZeroInFlightRequests,
    /// A deadline cannot be represented by the runtime clock.
    DeadlineTooLarge,
}

impl fmt::Display for BulkMutationPolicyIssue {
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
            Self::ZeroEntriesPerRequest => "max_entries_per_request must be greater than zero",
            Self::ZeroRequestBytes => "max_request_bytes must be greater than zero",
            Self::ZeroInFlightRequests => "max_in_flight_requests must be greater than zero",
            Self::DeadlineTooLarge => "deadline is too large for the runtime clock",
        })
    }
}

/// A malformed result in a streamed `MutateRows` response.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MutateRowsResponseIssue {
    /// A response index was negative.
    NegativeIndex {
        /// The invalid response index.
        index: i64,
    },
    /// A response index did not refer to an entry in the current request.
    IndexOutOfRange {
        /// The invalid response index.
        index: i64,
        /// Entries sent in the current request.
        entry_count: usize,
    },
    /// The stream reported one request entry more than once.
    DuplicateIndex {
        /// The repeated request-local index.
        index: usize,
    },
    /// The stream ended without reporting an entry.
    MissingIndex {
        /// The missing request-local index.
        index: usize,
    },
}

impl fmt::Display for MutateRowsResponseIssue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeIndex { index } => {
                write!(formatter, "response index {index} must not be negative")
            }
            Self::IndexOutOfRange { index, entry_count } => write!(
                formatter,
                "response index {index} is outside the {entry_count}-entry request"
            ),
            Self::DuplicateIndex { index } => {
                write!(
                    formatter,
                    "response index {index} was reported more than once"
                )
            }
            Self::MissingIndex { index } => {
                write!(formatter, "response index {index} was not reported")
            }
        }
    }
}

/// Why one bulk mutation entry lacks a confirmed success.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MutationFailureCause {
    /// Bigtable returned a non-OK status for this entry.
    #[error("entry failed: {status}")]
    EntryStatus {
        /// The entry status, including rich status details when present.
        #[source]
        status: tonic::Status,
        /// Whether replaying the entry is idempotent.
        retry_safe: bool,
    },
    /// The RPC ended before this entry received a result.
    #[error("mutation result was interrupted: {status}")]
    RpcStatus {
        /// The RPC status.
        #[source]
        status: tonic::Status,
        /// Whether replaying the entry is idempotent.
        retry_safe: bool,
    },
    /// The operation deadline ended before this entry completed.
    #[error("mutation exceeded the {timeout:?} operation deadline")]
    DeadlineExceeded {
        /// The configured operation timeout.
        timeout: std::time::Duration,
    },
    /// The response stream violated the `MutateRows` wire contract.
    #[error("invalid MutateRows response: {issue}")]
    InvalidResponse {
        /// The invalid index state.
        issue: MutateRowsResponseIssue,
    },
}

impl MutationFailureCause {
    /// Returns the gRPC status when the failure came from Bigtable or transport.
    #[must_use]
    pub const fn status(&self) -> Option<&tonic::Status> {
        match self {
            Self::EntryStatus { status, .. } | Self::RpcStatus { status, .. } => Some(status),
            Self::DeadlineExceeded { .. } | Self::InvalidResponse { .. } => None,
        }
    }

    /// Returns whether replaying the entry is idempotent.
    #[must_use]
    pub const fn retry_safe(&self) -> bool {
        match self {
            Self::EntryStatus { retry_safe, .. } | Self::RpcStatus { retry_safe, .. } => {
                *retry_safe
            }
            Self::DeadlineExceeded { .. } | Self::InvalidResponse { .. } => false,
        }
    }
}

/// One failed entry from the original bulk mutation.
#[derive(Debug)]
pub struct MutationFailure {
    pub(crate) index: usize,
    pub(crate) attempts: u32,
    pub(crate) cause: MutationFailureCause,
}

impl MutationFailure {
    /// Returns the entry index in the original bulk mutation.
    #[must_use]
    pub const fn index(&self) -> usize {
        self.index
    }

    /// Returns how many RPC attempts included this entry.
    #[must_use]
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Returns why the entry lacks a confirmed success.
    #[must_use]
    pub const fn cause(&self) -> &MutationFailureCause {
        &self.cause
    }
}

/// Grouped failures from one bulk mutation operation.
#[derive(Debug)]
pub struct BulkMutationError {
    pub(crate) total_entries: usize,
    pub(crate) successful_entries: usize,
    pub(crate) rpc_attempts: u32,
    pub(crate) failures: Vec<MutationFailure>,
}

impl BulkMutationError {
    /// Returns the number of entries in the original operation.
    #[must_use]
    pub const fn total_entries(&self) -> usize {
        self.total_entries
    }

    /// Returns the number of entries with confirmed success.
    #[must_use]
    pub const fn successful_entries(&self) -> usize {
        self.successful_entries
    }

    /// Returns the total RPC attempts across every request batch.
    #[must_use]
    pub const fn rpc_attempts(&self) -> u32 {
        self.rpc_attempts
    }

    /// Returns failures ordered by their original entry index.
    #[must_use]
    pub fn failures(&self) -> &[MutationFailure] {
        &self.failures
    }
}

impl fmt::Display for BulkMutationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Bigtable MutateRows confirmed {} of {} entries after {} RPC attempt(s); {} failed",
            self.successful_entries,
            self.total_entries,
            self.rpc_attempts,
            self.failures.len()
        )
    }
}

impl std::error::Error for BulkMutationError {}

/// The reason a high-level mutation is invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MutationIssue {
    /// The table ID is empty.
    EmptyTableId,
    /// The table ID exceeds Bigtable's 50-character limit.
    TableIdTooLong,
    /// The table ID contains a resource path separator.
    TableIdContainsSlash,
    /// The row key is empty.
    EmptyRowKey,
    /// The row key exceeds Bigtable's 4 KiB limit.
    RowKeyTooLong,
    /// A column family name is empty.
    EmptyFamilyName,
    /// A column family name contains an unsupported byte.
    InvalidFamilyName,
    /// A cell timestamp is negative.
    NegativeTimestamp,
    /// A cell timestamp does not use millisecond granularity.
    TimestampNotMillisecondAligned,
    /// A timestamp range is empty or reversed.
    InvalidTimestampRange,
    /// An advanced protobuf mutation has no operation.
    MissingOperation,
    /// A row entry has no mutations.
    EmptyRowMutation,
    /// A row entry exceeds the API mutation count limit.
    TooManyMutations,
    /// An idempotency token is shorter than eight bytes.
    IdempotencyTokenTooShort,
    /// The system clock is earlier than the Unix epoch.
    ClockBeforeUnixEpoch,
    /// The system clock cannot fit in a Bigtable timestamp.
    ClockOutOfRange,
}

impl fmt::Display for MutationIssue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyTableId => "table_id must not be empty",
            Self::TableIdTooLong => "table_id must not exceed 50 characters",
            Self::TableIdContainsSlash => "table_id must not contain '/'",
            Self::EmptyRowKey => "row key must not be empty",
            Self::RowKeyTooLong => "row key must not exceed 4 KiB",
            Self::EmptyFamilyName => "family_name must not be empty",
            Self::InvalidFamilyName => {
                "family_name may contain only ASCII letters, digits, '-', '_', and '.'"
            }
            Self::NegativeTimestamp => "timestamp_micros must not be negative",
            Self::TimestampNotMillisecondAligned => "timestamp_micros must be a multiple of 1000",
            Self::InvalidTimestampRange => "timestamp range start must be smaller than its end",
            Self::MissingOperation => "protobuf mutation must contain an operation",
            Self::EmptyRowMutation => "row mutation must contain at least one change",
            Self::TooManyMutations => "row mutation must not exceed 100000 changes",
            Self::IdempotencyTokenTooShort => "idempotency token must contain at least eight bytes",
            Self::ClockBeforeUnixEpoch => "system clock must not be earlier than the Unix epoch",
            Self::ClockOutOfRange => "system clock is too large for a Bigtable timestamp",
        })
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
    use super::{
        BulkMutationError, BulkMutationPolicyIssue, ConfigField, ConfigIssue, Error,
        MutateRowsResponseIssue, MutationFailure, MutationFailureCause, MutationIssue, QueryIssue,
        ReadPolicyIssue, RowMergeIssue,
    };

    #[test]
    fn invalid_config_display_names_the_field_and_issue() {
        let error = Error::invalid_config(ConfigField::ProjectId, ConfigIssue::Empty);

        assert_eq!(
            error.to_string(),
            "invalid Bigtable configuration for project_id: must not be empty"
        );
    }

    #[test]
    fn mutation_errors_include_recovery_context() {
        let error = Error::invalid_mutation(MutationIssue::EmptyRowMutation);

        assert_eq!(
            error.to_string(),
            "invalid Bigtable mutation: row mutation must contain at least one change"
        );
    }

    #[test]
    fn every_mutation_issue_has_clear_guidance() {
        let issues = [
            MutationIssue::EmptyTableId,
            MutationIssue::TableIdTooLong,
            MutationIssue::TableIdContainsSlash,
            MutationIssue::EmptyRowKey,
            MutationIssue::RowKeyTooLong,
            MutationIssue::EmptyFamilyName,
            MutationIssue::InvalidFamilyName,
            MutationIssue::NegativeTimestamp,
            MutationIssue::TimestampNotMillisecondAligned,
            MutationIssue::InvalidTimestampRange,
            MutationIssue::MissingOperation,
            MutationIssue::EmptyRowMutation,
            MutationIssue::TooManyMutations,
            MutationIssue::IdempotencyTokenTooShort,
            MutationIssue::ClockBeforeUnixEpoch,
            MutationIssue::ClockOutOfRange,
        ];

        for issue in issues {
            assert!(!issue.to_string().is_empty());
        }
    }

    #[test]
    fn bulk_mutation_policy_issues_have_clear_guidance() {
        let issues = [
            BulkMutationPolicyIssue::ZeroMaxAttempts,
            BulkMutationPolicyIssue::ZeroInitialBackoff,
            BulkMutationPolicyIssue::ZeroMaxBackoff,
            BulkMutationPolicyIssue::MaxBackoffTooSmall,
            BulkMutationPolicyIssue::InvalidBackoffMultiplier,
            BulkMutationPolicyIssue::ZeroOperationTimeout,
            BulkMutationPolicyIssue::ZeroAttemptTimeout,
            BulkMutationPolicyIssue::ZeroEntriesPerRequest,
            BulkMutationPolicyIssue::ZeroRequestBytes,
            BulkMutationPolicyIssue::ZeroInFlightRequests,
            BulkMutationPolicyIssue::DeadlineTooLarge,
        ];

        for issue in issues {
            assert!(!issue.to_string().is_empty());
        }
    }

    #[test]
    fn bulk_mutation_error_exposes_partial_success_and_failure_context() {
        let error = BulkMutationError {
            total_entries: 2,
            successful_entries: 1,
            rpc_attempts: 3,
            failures: vec![MutationFailure {
                index: 1,
                attempts: 2,
                cause: MutationFailureCause::EntryStatus {
                    status: tonic::Status::invalid_argument("bad cell"),
                    retry_safe: true,
                },
            }],
        };

        assert_eq!(error.total_entries(), 2);
        assert_eq!(error.successful_entries(), 1);
        assert_eq!(error.rpc_attempts(), 3);
        assert_eq!(error.failures()[0].index(), 1);
        assert_eq!(error.failures()[0].attempts(), 2);
        assert!(error.failures()[0].cause().retry_safe());
        assert_eq!(
            error.failures()[0]
                .cause()
                .status()
                .expect("entry status")
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            error.to_string(),
            "Bigtable MutateRows confirmed 1 of 2 entries after 3 RPC attempt(s); 1 failed"
        );
    }

    #[test]
    fn mutate_rows_response_issues_name_the_invalid_index_state() {
        let issues = [
            MutateRowsResponseIssue::NegativeIndex { index: -1 },
            MutateRowsResponseIssue::IndexOutOfRange {
                index: 4,
                entry_count: 2,
            },
            MutateRowsResponseIssue::DuplicateIndex { index: 1 },
            MutateRowsResponseIssue::MissingIndex { index: 0 },
        ];

        for issue in issues {
            assert!(!issue.to_string().is_empty());
        }
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
