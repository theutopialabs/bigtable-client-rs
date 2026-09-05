use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::{
    Error, MutationIssue,
    proto::{
        Idempotency, TimestampRange, mutate_rows_request,
        mutation::{
            DeleteFromColumn, DeleteFromFamily, DeleteFromRow, Mutation as ProtoMutation, SetCell,
        },
    },
    resource::{MAX_ROW_KEY_BYTES, TableIdIssue, validate_table_id},
};

const MAX_FAMILY_NAME_CHARS: usize = 64;
const MAX_MUTATIONS_PER_ENTRY: usize = 100_000;
const MIN_IDEMPOTENCY_TOKEN_BYTES: usize = 8;

/// One change applied as part of a row mutation.
#[derive(Clone, Debug, PartialEq)]
pub struct Mutation {
    inner: crate::proto::Mutation,
}

impl Mutation {
    /// Sets a cell using the current client time at millisecond precision.
    ///
    /// The fixed timestamp makes the mutation safe to retry.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidMutation`] when the family name is invalid or
    /// the system clock cannot produce a Bigtable timestamp.
    pub fn set_cell(
        family_name: impl Into<String>,
        column_qualifier: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<Self, Error> {
        let timestamp_micros = current_timestamp_micros()?;
        Self::set_cell_at(family_name, column_qualifier, timestamp_micros, value)
    }

    /// Sets a cell at a caller-provided timestamp.
    ///
    /// Bigtable tables use millisecond timestamp granularity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidMutation`] when the family name is invalid or
    /// the timestamp is negative or not aligned to a millisecond.
    pub fn set_cell_at(
        family_name: impl Into<String>,
        column_qualifier: impl Into<Bytes>,
        timestamp_micros: i64,
        value: impl Into<Bytes>,
    ) -> Result<Self, Error> {
        let family_name = valid_family_name(family_name.into())?;
        validate_timestamp(timestamp_micros)?;
        Ok(Self::new(ProtoMutation::SetCell(SetCell {
            family_name,
            column_qualifier: column_qualifier.into(),
            timestamp_micros,
            value: value.into(),
        })))
    }

    /// Sets a cell using Bigtable server time.
    ///
    /// A server-time write is not safe to retry after an ambiguous RPC
    /// failure. The bulk writer leaves that entry failed instead of replaying
    /// it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidMutation`] when the family name is invalid.
    pub fn set_cell_at_server_time(
        family_name: impl Into<String>,
        column_qualifier: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<Self, Error> {
        let family_name = valid_family_name(family_name.into())?;
        Ok(Self::new(ProtoMutation::SetCell(SetCell {
            family_name,
            column_qualifier: column_qualifier.into(),
            timestamp_micros: -1,
            value: value.into(),
        })))
    }

    /// Deletes every version of a cell.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidMutation`] when the family name is invalid.
    pub fn delete_cells(
        family_name: impl Into<String>,
        column_qualifier: impl Into<Bytes>,
    ) -> Result<Self, Error> {
        let family_name = valid_family_name(family_name.into())?;
        Ok(Self::new(ProtoMutation::DeleteFromColumn(
            DeleteFromColumn {
                family_name,
                column_qualifier: column_qualifier.into(),
                time_range: None,
            },
        )))
    }

    /// Deletes cell versions in a half-open timestamp range.
    ///
    /// `start_timestamp_micros` is inclusive and `end_timestamp_micros` is
    /// exclusive.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidMutation`] when the family or range is invalid.
    pub fn delete_cells_in_range(
        family_name: impl Into<String>,
        column_qualifier: impl Into<Bytes>,
        start_timestamp_micros: i64,
        end_timestamp_micros: i64,
    ) -> Result<Self, Error> {
        let family_name = valid_family_name(family_name.into())?;
        validate_timestamp(start_timestamp_micros)?;
        validate_timestamp(end_timestamp_micros)?;
        if start_timestamp_micros >= end_timestamp_micros {
            return Err(Error::invalid_mutation(
                MutationIssue::InvalidTimestampRange,
            ));
        }
        Ok(Self::new(ProtoMutation::DeleteFromColumn(
            DeleteFromColumn {
                family_name,
                column_qualifier: column_qualifier.into(),
                time_range: Some(TimestampRange {
                    start_timestamp_micros,
                    end_timestamp_micros,
                }),
            },
        )))
    }

    /// Deletes every cell in a column family.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidMutation`] when the family name is invalid.
    pub fn delete_family(family_name: impl Into<String>) -> Result<Self, Error> {
        let family_name = valid_family_name(family_name.into())?;
        Ok(Self::new(ProtoMutation::DeleteFromFamily(
            DeleteFromFamily { family_name },
        )))
    }

    /// Deletes every cell in a row.
    #[must_use]
    pub fn delete_row() -> Self {
        Self::new(ProtoMutation::DeleteFromRow(DeleteFromRow {}))
    }

    /// Wraps an advanced protobuf mutation.
    ///
    /// The bulk writer detects retry safety from the wrapped mutation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidMutation`] when no mutation operation is set.
    pub fn from_proto(mutation: crate::proto::Mutation) -> Result<Self, Error> {
        if mutation.mutation.is_none() {
            return Err(Error::invalid_mutation(MutationIssue::MissingOperation));
        }
        Ok(Self { inner: mutation })
    }

    fn new(mutation: ProtoMutation) -> Self {
        Self {
            inner: crate::proto::Mutation {
                mutation: Some(mutation),
            },
        }
    }

    fn is_retry_safe(&self, has_idempotency: bool) -> bool {
        match self.inner.mutation.as_ref() {
            Some(ProtoMutation::SetCell(cell)) => cell.timestamp_micros != -1,
            Some(
                ProtoMutation::DeleteFromColumn(_)
                | ProtoMutation::DeleteFromFamily(_)
                | ProtoMutation::DeleteFromRow(_),
            ) => true,
            Some(ProtoMutation::AddToCell(_) | ProtoMutation::MergeToCell(_)) => has_idempotency,
            None => false,
        }
    }
}

/// An ordered set of changes applied atomically to one row.
#[derive(Clone, Debug, PartialEq)]
pub struct RowMutation {
    row_key: Bytes,
    mutations: Vec<Mutation>,
    idempotency: Option<Idempotency>,
}

impl RowMutation {
    /// Creates an empty mutation for one row.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidMutation`] when the row key is empty or larger
    /// than 4 KiB.
    pub fn new(row_key: impl Into<Bytes>) -> Result<Self, Error> {
        let row_key = row_key.into();
        validate_row_key(&row_key)?;
        Ok(Self {
            row_key,
            mutations: Vec::new(),
            idempotency: None,
        })
    }

    /// Appends one change to this row.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidMutation`] when the row would exceed the API
    /// limit of 100,000 changes.
    pub fn mutation(mut self, mutation: Mutation) -> Result<Self, Error> {
        if self.mutations.len() == MAX_MUTATIONS_PER_ENTRY {
            return Err(Error::invalid_mutation(MutationIssue::TooManyMutations));
        }
        self.mutations.push(mutation);
        Ok(self)
    }

    /// Adds a stable token for aggregate mutation retries.
    ///
    /// Bigtable keeps token protection for a limited window. The client records
    /// the current time so a late replay can be rejected.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidMutation`] when the token is shorter than eight
    /// bytes or the system clock cannot produce a protobuf timestamp.
    pub fn with_idempotency_token(mut self, token: impl Into<Bytes>) -> Result<Self, Error> {
        let token = token.into();
        if token.len() < MIN_IDEMPOTENCY_TOKEN_BYTES {
            return Err(Error::invalid_mutation(
                MutationIssue::IdempotencyTokenTooShort,
            ));
        }
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::invalid_mutation(MutationIssue::ClockBeforeUnixEpoch))?;
        let seconds = i64::try_from(elapsed.as_secs())
            .map_err(|_| Error::invalid_mutation(MutationIssue::ClockOutOfRange))?;
        let nanos = i32::try_from(elapsed.subsec_nanos())
            .map_err(|_| Error::invalid_mutation(MutationIssue::ClockOutOfRange))?;
        self.idempotency = Some(Idempotency {
            token,
            start_time: Some(prost_types::Timestamp { seconds, nanos }),
        });
        Ok(self)
    }

    /// Returns the raw row key.
    #[must_use]
    pub fn row_key(&self) -> &Bytes {
        &self.row_key
    }

    /// Returns the number of changes in this row mutation.
    #[must_use]
    pub fn len(&self) -> usize {
        self.mutations.len()
    }

    /// Returns whether this row mutation has no changes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }

    /// Returns whether an ambiguous attempt can be retried safely.
    #[must_use]
    pub fn is_retry_safe(&self) -> bool {
        self.mutations
            .iter()
            .all(|mutation| mutation.is_retry_safe(self.idempotency.is_some()))
    }

    pub(crate) fn into_proto(self) -> mutate_rows_request::Entry {
        mutate_rows_request::Entry {
            row_key: self.row_key,
            mutations: self
                .mutations
                .into_iter()
                .map(|mutation| mutation.inner)
                .collect(),
            idempotency: self.idempotency,
        }
    }
}

/// Mutations for rows in one table.
#[derive(Clone, Debug, PartialEq)]
pub struct BulkMutation {
    table_id: String,
    entries: Vec<RowMutation>,
}

impl BulkMutation {
    /// Creates an empty bulk mutation for a table.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidMutation`] when the table ID is invalid.
    pub fn new(table_id: impl Into<String>) -> Result<Self, Error> {
        let table_id = table_id.into();
        validate_table_id(&table_id).map_err(|issue| {
            Error::invalid_mutation(match issue {
                TableIdIssue::Empty => MutationIssue::EmptyTableId,
                TableIdIssue::TooLong => MutationIssue::TableIdTooLong,
                TableIdIssue::ContainsSlash => MutationIssue::TableIdContainsSlash,
            })
        })?;
        Ok(Self {
            table_id,
            entries: Vec::new(),
        })
    }

    /// Appends one row mutation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidMutation`] when the row mutation has no changes.
    pub fn push(&mut self, entry: RowMutation) -> Result<(), Error> {
        if entry.is_empty() {
            return Err(Error::invalid_mutation(MutationIssue::EmptyRowMutation));
        }
        self.entries.push(entry);
        Ok(())
    }

    /// Appends one row mutation and returns this builder.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidMutation`] when the row mutation has no changes.
    pub fn entry(mut self, entry: RowMutation) -> Result<Self, Error> {
        self.push(entry)?;
        Ok(self)
    }

    /// Returns the target table ID.
    #[must_use]
    pub fn table_id(&self) -> &str {
        &self.table_id
    }

    /// Returns the number of row entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether this bulk mutation has no row entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn into_parts(self) -> (String, Vec<RowMutation>) {
        (self.table_id, self.entries)
    }
}

fn current_timestamp_micros() -> Result<i64, Error> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::invalid_mutation(MutationIssue::ClockBeforeUnixEpoch))?;
    let millis = i64::try_from(elapsed.as_millis())
        .map_err(|_| Error::invalid_mutation(MutationIssue::ClockOutOfRange))?;
    millis
        .checked_mul(1_000)
        .ok_or_else(|| Error::invalid_mutation(MutationIssue::ClockOutOfRange))
}

fn validate_row_key(row_key: &Bytes) -> Result<(), Error> {
    if row_key.is_empty() {
        return Err(Error::invalid_mutation(MutationIssue::EmptyRowKey));
    }
    if row_key.len() > MAX_ROW_KEY_BYTES {
        return Err(Error::invalid_mutation(MutationIssue::RowKeyTooLong));
    }
    Ok(())
}

fn valid_family_name(family_name: String) -> Result<String, Error> {
    if family_name.is_empty() {
        return Err(Error::invalid_mutation(MutationIssue::EmptyFamilyName));
    }
    if family_name.chars().count() > MAX_FAMILY_NAME_CHARS {
        return Err(Error::invalid_mutation(MutationIssue::FamilyNameTooLong));
    }
    if !family_name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(Error::invalid_mutation(MutationIssue::InvalidFamilyName));
    }
    Ok(family_name)
}

fn validate_timestamp(timestamp_micros: i64) -> Result<(), Error> {
    if timestamp_micros < 0 {
        return Err(Error::invalid_mutation(MutationIssue::NegativeTimestamp));
    }
    if timestamp_micros % 1_000 != 0 {
        return Err(Error::invalid_mutation(
            MutationIssue::TimestampNotMillisecondAligned,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{BulkMutation, Mutation, RowMutation};
    use crate::{
        Error, MutationIssue,
        proto::{
            Mutation as ProtoMutation,
            mutation::{AddToCell, Mutation as MutationKind, SetCell},
        },
    };

    fn set_cell() -> Mutation {
        Mutation::set_cell_at("cf", b"q".to_vec(), 1_000, b"value".to_vec()).expect("valid cell")
    }

    #[test]
    fn cell_mutations_validate_family_and_timestamp() {
        assert!(matches!(
            Mutation::set_cell_at("", Bytes::new(), 0, Bytes::new()),
            Err(Error::InvalidMutation {
                issue: MutationIssue::EmptyFamilyName
            })
        ));
        assert!(matches!(
            Mutation::set_cell_at("a".repeat(65), Bytes::new(), 0, Bytes::new()),
            Err(Error::InvalidMutation {
                issue: MutationIssue::FamilyNameTooLong
            })
        ));
        assert!(matches!(
            Mutation::set_cell_at("bad/family", Bytes::new(), 0, Bytes::new()),
            Err(Error::InvalidMutation {
                issue: MutationIssue::InvalidFamilyName
            })
        ));
        assert!(matches!(
            Mutation::set_cell_at("cf", Bytes::new(), -1, Bytes::new()),
            Err(Error::InvalidMutation {
                issue: MutationIssue::NegativeTimestamp
            })
        ));
        assert!(matches!(
            Mutation::set_cell_at("cf", Bytes::new(), 1, Bytes::new()),
            Err(Error::InvalidMutation {
                issue: MutationIssue::TimestampNotMillisecondAligned
            })
        ));
    }

    #[test]
    fn set_cell_uses_a_retry_safe_client_timestamp() {
        let mutation =
            Mutation::set_cell("cf", b"q".to_vec(), b"value".to_vec()).expect("current time");
        let row = RowMutation::new(b"row".to_vec())
            .expect("valid row")
            .mutation(mutation)
            .expect("within limit");

        assert!(row.is_retry_safe());
        let entry = row.into_proto();
        let Some(MutationKind::SetCell(cell)) = entry.mutations[0].mutation.as_ref() else {
            panic!("set cell");
        };
        assert!(cell.timestamp_micros >= 0);
        assert_eq!(cell.timestamp_micros % 1_000, 0);
    }

    #[test]
    fn server_timestamp_is_explicit_and_not_retry_safe() {
        let mutation = Mutation::set_cell_at_server_time("cf", b"q".to_vec(), b"value".to_vec())
            .expect("valid cell");
        let row = RowMutation::new(b"row".to_vec())
            .expect("valid row")
            .mutation(mutation)
            .expect("within limit");

        assert!(!row.is_retry_safe());
    }

    #[test]
    fn delete_builders_cover_cell_range_family_and_row() {
        let row = RowMutation::new(b"row".to_vec())
            .expect("valid row")
            .mutation(Mutation::delete_cells("cf", b"all".to_vec()).expect("valid family"))
            .expect("within limit")
            .mutation(
                Mutation::delete_cells_in_range("cf", b"range".to_vec(), 1_000, 2_000)
                    .expect("valid range"),
            )
            .expect("within limit")
            .mutation(Mutation::delete_family("cf").expect("valid family"))
            .expect("within limit")
            .mutation(Mutation::delete_row())
            .expect("within limit");

        assert_eq!(row.len(), 4);
        assert!(row.is_retry_safe());
        assert!(matches!(
            Mutation::delete_cells_in_range("cf", Bytes::new(), 2_000, 1_000),
            Err(Error::InvalidMutation {
                issue: MutationIssue::InvalidTimestampRange
            })
        ));
    }

    #[test]
    fn advanced_proto_requires_an_operation() {
        assert!(matches!(
            Mutation::from_proto(ProtoMutation::default()),
            Err(Error::InvalidMutation {
                issue: MutationIssue::MissingOperation
            })
        ));
    }

    #[test]
    fn idempotency_token_makes_aggregate_mutation_retry_safe() {
        let aggregate = Mutation::from_proto(ProtoMutation {
            mutation: Some(MutationKind::AddToCell(AddToCell::default())),
        })
        .expect("operation is set");
        let unsafe_row = RowMutation::new(b"row".to_vec())
            .expect("valid row")
            .mutation(aggregate.clone())
            .expect("within limit");
        assert!(!unsafe_row.is_retry_safe());

        let safe_row = RowMutation::new(b"row".to_vec())
            .expect("valid row")
            .mutation(aggregate)
            .expect("within limit")
            .with_idempotency_token(b"12345678".to_vec())
            .expect("valid token");
        assert!(safe_row.is_retry_safe());
        assert!(safe_row.into_proto().idempotency.is_some());
    }

    #[test]
    fn row_and_token_validation_return_typed_errors() {
        assert!(matches!(
            RowMutation::new(Bytes::new()),
            Err(Error::InvalidMutation {
                issue: MutationIssue::EmptyRowKey
            })
        ));
        assert!(matches!(
            RowMutation::new(vec![0; 4 * 1024 + 1]),
            Err(Error::InvalidMutation {
                issue: MutationIssue::RowKeyTooLong
            })
        ));
        assert!(matches!(
            RowMutation::new(b"row".to_vec())
                .expect("valid row")
                .with_idempotency_token(b"short".to_vec()),
            Err(Error::InvalidMutation {
                issue: MutationIssue::IdempotencyTokenTooShort
            })
        ));
    }

    #[test]
    fn bulk_mutation_supports_chaining_and_mutable_push() {
        let first = RowMutation::new(b"one".to_vec())
            .expect("valid row")
            .mutation(set_cell())
            .expect("within limit");
        let second = RowMutation::new(b"two".to_vec())
            .expect("valid row")
            .mutation(set_cell())
            .expect("within limit");
        let mut bulk = BulkMutation::new("events")
            .expect("valid table")
            .entry(first)
            .expect("nonempty row");
        bulk.push(second).expect("nonempty row");

        assert_eq!(bulk.table_id(), "events");
        assert_eq!(bulk.len(), 2);
        assert!(!bulk.is_empty());
    }

    #[test]
    fn bulk_mutation_rejects_invalid_tables_and_empty_rows() {
        assert!(matches!(
            BulkMutation::new(""),
            Err(Error::InvalidMutation {
                issue: MutationIssue::EmptyTableId
            })
        ));
        assert!(matches!(
            BulkMutation::new("a/b"),
            Err(Error::InvalidMutation {
                issue: MutationIssue::TableIdContainsSlash
            })
        ));
        assert!(matches!(
            BulkMutation::new("a".repeat(51)),
            Err(Error::InvalidMutation {
                issue: MutationIssue::TableIdTooLong
            })
        ));
        let empty = RowMutation::new(b"row".to_vec()).expect("valid row");
        assert!(matches!(
            BulkMutation::new("events")
                .expect("valid table")
                .entry(empty),
            Err(Error::InvalidMutation {
                issue: MutationIssue::EmptyRowMutation
            })
        ));
    }

    #[test]
    fn row_accessors_report_key_and_empty_state() {
        let row = RowMutation::new(b"row".to_vec()).expect("valid row");

        assert_eq!(row.row_key(), &Bytes::from_static(b"row"));
        assert_eq!(row.len(), 0);
        assert!(row.is_empty());
    }

    #[test]
    fn row_mutation_enforces_the_api_change_limit() {
        let mut row = RowMutation::new(b"row".to_vec()).expect("valid row");
        for _ in 0..100_000 {
            row = row.mutation(Mutation::delete_row()).expect("within limit");
        }
        assert!(matches!(
            row.mutation(Mutation::delete_row()),
            Err(Error::InvalidMutation {
                issue: MutationIssue::TooManyMutations
            })
        ));
    }

    #[test]
    fn set_cell_proto_values_are_preserved() {
        let mutation = Mutation::set_cell_at("cf", b"q".to_vec(), 2_000, b"value".to_vec())
            .expect("valid cell");
        let row = RowMutation::new(b"row".to_vec())
            .expect("valid row")
            .mutation(mutation)
            .expect("within limit");
        let entry = row.into_proto();

        assert_eq!(entry.row_key, Bytes::from_static(b"row"));
        assert_eq!(entry.mutations.len(), 1);
        assert_eq!(
            entry.mutations[0],
            ProtoMutation {
                mutation: Some(MutationKind::SetCell(SetCell {
                    family_name: "cf".to_owned(),
                    column_qualifier: Bytes::from_static(b"q"),
                    timestamp_micros: 2_000,
                    value: Bytes::from_static(b"value"),
                }))
            }
        );
    }
}
