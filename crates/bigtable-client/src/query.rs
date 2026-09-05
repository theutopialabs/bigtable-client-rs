use bytes::Bytes;

use crate::{
    ClientConfig, Error, QueryIssue,
    proto::{
        ReadRowsRequest, RowFilter, RowSet,
        row_range::{EndKey, StartKey},
    },
    resource::{MAX_ROW_KEY_BYTES, TableIdIssue, table_name, validate_table_id},
};

/// One end of a row range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowBound {
    /// No bound.
    Unbounded,
    /// Include this row key.
    Inclusive(Bytes),
    /// Exclude this row key.
    Exclusive(Bytes),
}

impl RowBound {
    /// Creates an inclusive bound.
    #[must_use]
    pub fn inclusive(key: impl Into<Bytes>) -> Self {
        Self::Inclusive(key.into())
    }

    /// Creates an exclusive bound.
    #[must_use]
    pub fn exclusive(key: impl Into<Bytes>) -> Self {
        Self::Exclusive(key.into())
    }
}

/// A contiguous row key range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowRange {
    /// The lower bound.
    pub start: RowBound,
    /// The upper bound.
    pub end: RowBound,
}

impl RowRange {
    /// Creates a range from explicit bounds.
    #[must_use]
    pub const fn new(start: RowBound, end: RowBound) -> Self {
        Self { start, end }
    }

    /// Creates a range that includes every key with this prefix.
    #[must_use]
    pub fn prefix(prefix: impl Into<Bytes>) -> Self {
        let prefix = prefix.into();
        let end = prefix_successor(&prefix).map_or(RowBound::Unbounded, RowBound::Exclusive);
        Self::new(RowBound::Inclusive(prefix), end)
    }
}

/// A high-level `ReadRows` query.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    table_id: String,
    row_keys: Vec<Bytes>,
    row_ranges: Vec<RowRange>,
    filter: Option<RowFilter>,
    limit: Option<i64>,
    reversed: bool,
}

impl Query {
    /// Creates a query for a table.
    ///
    /// A query reads the whole table until keys, ranges, or a prefix are added.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidQuery`] when the table ID is empty, longer than
    /// 50 characters, or contains `/`.
    pub fn new(table_id: impl Into<String>) -> Result<Self, Error> {
        let table_id = table_id.into();
        validate_table_id(&table_id).map_err(|issue| {
            Error::invalid_query(match issue {
                TableIdIssue::Empty => QueryIssue::EmptyTableId,
                TableIdIssue::TooLong => QueryIssue::TableIdTooLong,
                TableIdIssue::ContainsSlash => QueryIssue::TableIdContainsSlash,
            })
        })?;

        Ok(Self {
            table_id,
            row_keys: Vec::new(),
            row_ranges: Vec::new(),
            filter: None,
            limit: None,
            reversed: false,
        })
    }

    /// Adds one exact row key.
    #[must_use]
    pub fn row_key(mut self, key: impl Into<Bytes>) -> Self {
        self.row_keys.push(key.into());
        self
    }

    /// Adds exact row keys.
    #[must_use]
    pub fn row_keys<I, K>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = K>,
        K: Into<Bytes>,
    {
        self.row_keys.extend(keys.into_iter().map(Into::into));
        self
    }

    /// Adds one row range.
    #[must_use]
    pub fn row_range(mut self, range: RowRange) -> Self {
        self.row_ranges.push(range);
        self
    }

    /// Adds every row key with this prefix.
    #[must_use]
    pub fn prefix(self, prefix: impl Into<Bytes>) -> Self {
        self.row_range(RowRange::prefix(prefix))
    }

    /// Sets the server-side row filter.
    #[must_use]
    pub fn filter(mut self, filter: RowFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Limits the number of committed rows returned.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidQuery`] when the limit is zero or larger than
    /// the Bigtable API can represent.
    pub fn limit(mut self, limit: u64) -> Result<Self, Error> {
        if limit == 0 {
            return Err(Error::invalid_query(QueryIssue::ZeroRowLimit));
        }
        self.limit = Some(
            i64::try_from(limit).map_err(|_| Error::invalid_query(QueryIssue::RowLimitTooLarge))?,
        );
        Ok(self)
    }

    /// Returns rows in descending row key order.
    #[must_use]
    pub fn reversed(mut self) -> Self {
        self.reversed = true;
        self
    }

    pub(crate) fn table_id(&self) -> &str {
        &self.table_id
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        for key in &self.row_keys {
            if key.is_empty() {
                return Err(Error::invalid_query(QueryIssue::EmptyRowKey));
            }
            validate_key_size(key)?;
        }
        for range in &self.row_ranges {
            validate_bound_size(&range.start)?;
            validate_bound_size(&range.end)?;
        }
        Ok(())
    }

    pub(crate) fn into_request(self, config: &ClientConfig) -> ReadRowsRequest {
        let rows = if self.row_keys.is_empty() && self.row_ranges.is_empty() {
            None
        } else {
            Some(RowSet {
                row_keys: self.row_keys,
                row_ranges: self.row_ranges.into_iter().map(proto_row_range).collect(),
            })
        };

        ReadRowsRequest {
            table_name: table_name(config, &self.table_id),
            app_profile_id: config.app_profile_id().to_owned(),
            rows,
            filter: self.filter,
            rows_limit: self.limit.unwrap_or(0),
            reversed: self.reversed,
            ..ReadRowsRequest::default()
        }
    }
}

fn validate_bound_size(bound: &RowBound) -> Result<(), Error> {
    match bound {
        RowBound::Unbounded => Ok(()),
        RowBound::Inclusive(key) | RowBound::Exclusive(key) => validate_key_size(key),
    }
}

fn validate_key_size(key: &Bytes) -> Result<(), Error> {
    if key.len() > MAX_ROW_KEY_BYTES {
        return Err(Error::invalid_query(QueryIssue::RowKeyTooLong));
    }
    Ok(())
}

fn prefix_successor(prefix: &Bytes) -> Option<Bytes> {
    let mut end = prefix.to_vec();
    let index = end.iter().rposition(|byte| *byte != u8::MAX)?;
    end[index] += 1;
    end.truncate(index + 1);
    Some(end.into())
}

fn proto_row_range(range: RowRange) -> crate::proto::RowRange {
    crate::proto::RowRange {
        start_key: match range.start {
            RowBound::Unbounded => None,
            RowBound::Inclusive(key) => Some(StartKey::StartKeyClosed(key)),
            RowBound::Exclusive(key) => Some(StartKey::StartKeyOpen(key)),
        },
        end_key: match range.end {
            RowBound::Unbounded => None,
            RowBound::Inclusive(key) => Some(EndKey::EndKeyClosed(key)),
            RowBound::Exclusive(key) => Some(EndKey::EndKeyOpen(key)),
        },
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{Query, RowBound, RowRange};
    use crate::{
        ClientConfig, Error, QueryIssue,
        proto::{
            RowFilter,
            row_filter::Filter,
            row_range::{EndKey, StartKey},
        },
    };

    fn config() -> ClientConfig {
        ClientConfig::new("project", "instance")
            .expect("valid config")
            .with_app_profile_id("analytics")
            .expect("valid app profile")
    }

    #[test]
    fn empty_query_reads_the_whole_table() {
        let request = Query::new("table")
            .expect("valid table")
            .into_request(&config());

        assert_eq!(
            request.table_name,
            "projects/project/instances/instance/tables/table"
        );
        assert_eq!(request.app_profile_id, "analytics");
        assert!(request.rows.is_none());
        assert_eq!(request.rows_limit, 0);
        assert!(!request.reversed);
    }

    #[test]
    fn query_builds_keys_ranges_filter_limit_and_reverse_order() {
        let filter = RowFilter {
            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
        };
        let query = Query::new("table")
            .expect("valid table")
            .row_key(Bytes::from_static(b"one"))
            .row_keys([Bytes::from_static(b"two"), Bytes::from_static(b"three")])
            .row_range(RowRange::new(
                RowBound::inclusive(Bytes::from_static(b"a")),
                RowBound::exclusive(Bytes::from_static(b"z")),
            ))
            .filter(filter.clone())
            .limit(7)
            .expect("valid limit")
            .reversed();
        let request = query.into_request(&config());
        let rows = request.rows.expect("selected rows");

        assert_eq!(
            rows.row_keys,
            vec![
                Bytes::from_static(b"one"),
                Bytes::from_static(b"two"),
                Bytes::from_static(b"three")
            ]
        );
        assert_eq!(
            rows.row_ranges[0].start_key,
            Some(StartKey::StartKeyClosed(Bytes::from_static(b"a")))
        );
        assert_eq!(
            rows.row_ranges[0].end_key,
            Some(EndKey::EndKeyOpen(Bytes::from_static(b"z")))
        );
        assert_eq!(request.filter, Some(filter));
        assert_eq!(request.rows_limit, 7);
        assert!(request.reversed);
    }

    #[test]
    fn prefix_uses_the_smallest_lexicographic_successor() {
        let request = Query::new("table")
            .expect("valid table")
            .prefix(Bytes::from_static(b"ab\xff"))
            .into_request(&config());
        let range = &request.rows.expect("prefix range").row_ranges[0];

        assert_eq!(
            range.start_key,
            Some(StartKey::StartKeyClosed(Bytes::from_static(b"ab\xff")))
        );
        assert_eq!(
            range.end_key,
            Some(EndKey::EndKeyOpen(Bytes::from_static(b"ac")))
        );
    }

    #[test]
    fn all_ff_prefix_has_an_unbounded_end() {
        let request = Query::new("table")
            .expect("valid table")
            .prefix(Bytes::from_static(b"\xff\xff"))
            .into_request(&config());
        let range = &request.rows.expect("prefix range").row_ranges[0];

        assert!(range.end_key.is_none());
    }

    #[test]
    fn explicit_inclusive_and_exclusive_bounds_are_preserved() {
        assert_eq!(
            RowBound::inclusive(Bytes::from_static(b"a")),
            RowBound::Inclusive(Bytes::from_static(b"a"))
        );
        assert_eq!(
            RowBound::exclusive(Bytes::from_static(b"z")),
            RowBound::Exclusive(Bytes::from_static(b"z"))
        );
        assert_eq!(
            RowRange::new(RowBound::Unbounded, RowBound::Unbounded),
            RowRange {
                start: RowBound::Unbounded,
                end: RowBound::Unbounded
            }
        );
    }

    #[test]
    fn invalid_table_ids_return_typed_errors() {
        let cases = [
            (String::new(), QueryIssue::EmptyTableId),
            ("a".repeat(51), QueryIssue::TableIdTooLong),
            ("table/child".to_owned(), QueryIssue::TableIdContainsSlash),
        ];

        for (table_id, expected) in cases {
            let error = Query::new(table_id).expect_err("invalid table ID");
            assert!(matches!(
                error,
                Error::InvalidQuery { issue } if issue == expected
            ));
        }
    }

    #[test]
    fn invalid_limits_return_typed_errors() {
        let zero = Query::new("table")
            .expect("valid table")
            .limit(0)
            .expect_err("zero limit");
        let too_large = Query::new("table")
            .expect("valid table")
            .limit(u64::MAX)
            .expect_err("oversized limit");

        assert!(matches!(
            zero,
            Error::InvalidQuery {
                issue: QueryIssue::ZeroRowLimit
            }
        ));
        assert!(matches!(
            too_large,
            Error::InvalidQuery {
                issue: QueryIssue::RowLimitTooLarge
            }
        ));
    }

    #[test]
    fn invalid_read_keys_return_typed_errors() {
        let empty = Query::new("table")
            .expect("valid table")
            .row_key(Bytes::new())
            .validate()
            .expect_err("empty exact key");
        let long_key = Bytes::from(vec![0; 4 * 1024 + 1]);
        let exact = Query::new("table")
            .expect("valid table")
            .row_key(long_key.clone())
            .validate()
            .expect_err("oversized exact key");
        let range = Query::new("table")
            .expect("valid table")
            .row_range(RowRange::new(
                RowBound::Unbounded,
                RowBound::exclusive(long_key),
            ))
            .validate()
            .expect_err("oversized range bound");

        assert!(matches!(
            empty,
            Error::InvalidQuery {
                issue: QueryIssue::EmptyRowKey
            }
        ));
        for error in [exact, range] {
            assert!(matches!(
                error,
                Error::InvalidQuery {
                    issue: QueryIssue::RowKeyTooLong
                }
            ));
        }
        Query::new("table")
            .expect("valid table")
            .row_range(RowRange::new(
                RowBound::inclusive(Bytes::new()),
                RowBound::Unbounded,
            ))
            .validate()
            .expect("empty range bound stays valid");
    }
}
