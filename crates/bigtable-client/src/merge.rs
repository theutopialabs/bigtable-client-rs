use bytes::{Bytes, BytesMut};

use crate::{
    Cell, Column, Family, Row, RowMergeIssue,
    proto::read_rows_response::{CellChunk, cell_chunk::RowStatus},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    NewRow,
    NewCell,
    CellValue,
}

pub(crate) struct RowMerger {
    reversed: bool,
    state: State,
    last_complete_key: Option<Bytes>,
    row: Row,
    family: String,
    qualifier: Bytes,
    timestamp_micros: i64,
    labels: Vec<String>,
    value: BytesMut,
    expected_value_size: usize,
    remaining_value_bytes: usize,
}

impl RowMerger {
    pub(crate) fn new(reversed: bool) -> Self {
        Self {
            reversed,
            state: State::NewRow,
            last_complete_key: None,
            row: empty_row(),
            family: String::new(),
            qualifier: Bytes::new(),
            timestamp_micros: 0,
            labels: Vec::new(),
            value: BytesMut::new(),
            expected_value_size: 0,
            remaining_value_bytes: 0,
        }
    }

    pub(crate) fn push(&mut self, chunk: &CellChunk) -> Result<Option<Row>, RowMergeIssue> {
        match self.state {
            State::NewRow => self.start_row(chunk),
            State::NewCell => self.start_cell(chunk),
            State::CellValue => self.continue_cell(chunk),
        }
    }

    pub(crate) fn scan_marker(&mut self, key: Bytes) -> Result<(), RowMergeIssue> {
        if self.state != State::NewRow {
            return Err(RowMergeIssue::ScanMarkerDuringRow);
        }
        self.validate_order(&key)?;
        self.last_complete_key = Some(key);
        Ok(())
    }

    pub(crate) fn discard_partial(&mut self) {
        if self.state != State::NewRow {
            self.clear_active_row();
        }
    }

    pub(crate) fn finish(&self) -> Result<(), RowMergeIssue> {
        if self.state == State::NewRow {
            Ok(())
        } else {
            Err(RowMergeIssue::IncompleteRow)
        }
    }

    fn start_row(&mut self, chunk: &CellChunk) -> Result<Option<Row>, RowMergeIssue> {
        if matches!(chunk.row_status, Some(RowStatus::ResetRow(true))) {
            return Err(RowMergeIssue::ResetBetweenRows);
        }
        if chunk.row_key.is_empty() {
            return Err(RowMergeIssue::MissingRowKey);
        }
        let family = chunk
            .family_name
            .as_ref()
            .ok_or(RowMergeIssue::MissingFamily)?;
        let qualifier = chunk
            .qualifier
            .as_ref()
            .ok_or(RowMergeIssue::MissingQualifier)?;
        self.validate_order(&chunk.row_key)?;

        self.row = Row {
            key: chunk.row_key.clone(),
            families: Vec::new(),
        };
        self.family.clone_from(family);
        self.qualifier = Bytes::copy_from_slice(qualifier);
        self.state = State::NewCell;
        self.start_cell(chunk)
    }

    fn start_cell(&mut self, chunk: &CellChunk) -> Result<Option<Row>, RowMergeIssue> {
        if matches!(chunk.row_status, Some(RowStatus::ResetRow(true))) {
            Self::validate_reset(chunk)?;
            self.clear_active_row();
            return Ok(None);
        }
        if !chunk.row_key.is_empty() && chunk.row_key != self.row.key {
            return Err(RowMergeIssue::RowKeyChanged);
        }
        if chunk.family_name.is_some() && chunk.qualifier.is_none() {
            return Err(RowMergeIssue::FamilyWithoutQualifier);
        }
        if let Some(family) = &chunk.family_name {
            self.family.clone_from(family);
        }
        if let Some(qualifier) = &chunk.qualifier {
            self.qualifier = Bytes::copy_from_slice(qualifier);
        }
        if chunk.value_size < 0 {
            return Err(RowMergeIssue::NegativeValueSize);
        }

        self.timestamp_micros = chunk.timestamp_micros;
        self.labels.clone_from(&chunk.labels);
        self.value.clear();

        if chunk.value_size > 0 {
            if chunk.value.is_empty() {
                return Err(RowMergeIssue::SplitValueMissingData);
            }
            if matches!(chunk.row_status, Some(RowStatus::CommitRow(true))) {
                return Err(RowMergeIssue::CommitBeforeCellComplete);
            }
            let expected =
                usize::try_from(chunk.value_size).map_err(|_| RowMergeIssue::NegativeValueSize)?;
            if chunk.value.len() > expected {
                return Err(RowMergeIssue::SplitValueTooLarge);
            }
            self.value.reserve(expected);
            self.value.extend_from_slice(&chunk.value);
            self.expected_value_size = expected;
            self.remaining_value_bytes = expected - chunk.value.len();
            self.state = State::CellValue;
            return Ok(None);
        }

        self.value.extend_from_slice(&chunk.value);
        self.finish_cell();
        if matches!(chunk.row_status, Some(RowStatus::CommitRow(true))) {
            return Ok(Some(self.commit_row()));
        }
        self.state = State::NewCell;
        Ok(None)
    }

    fn continue_cell(&mut self, chunk: &CellChunk) -> Result<Option<Row>, RowMergeIssue> {
        if matches!(chunk.row_status, Some(RowStatus::ResetRow(true))) {
            Self::validate_reset(chunk)?;
            self.clear_active_row();
            return Ok(None);
        }
        if !chunk.row_key.is_empty()
            || chunk.family_name.is_some()
            || chunk.qualifier.is_some()
            || chunk.timestamp_micros != 0
            || !chunk.labels.is_empty()
        {
            return Err(RowMergeIssue::CellMetadataOnContinuation);
        }
        if chunk.value_size < 0 {
            return Err(RowMergeIssue::NegativeValueSize);
        }
        if chunk.value.len() > self.remaining_value_bytes {
            return Err(RowMergeIssue::SplitValueTooLarge);
        }

        let terminal = chunk.value_size == 0;
        if terminal {
            if chunk.value.len() != self.remaining_value_bytes {
                return Err(RowMergeIssue::SplitValueWrongSize);
            }
        } else {
            let value_size =
                usize::try_from(chunk.value_size).map_err(|_| RowMergeIssue::NegativeValueSize)?;
            if value_size != self.expected_value_size {
                return Err(RowMergeIssue::SplitValueSizeChanged);
            }
            if matches!(chunk.row_status, Some(RowStatus::CommitRow(true))) {
                return Err(RowMergeIssue::CommitBeforeCellComplete);
            }
        }

        self.value.extend_from_slice(&chunk.value);
        self.remaining_value_bytes -= chunk.value.len();
        if !terminal {
            return Ok(None);
        }

        self.finish_cell();
        if matches!(chunk.row_status, Some(RowStatus::CommitRow(true))) {
            return Ok(Some(self.commit_row()));
        }
        self.state = State::NewCell;
        Ok(None)
    }

    fn finish_cell(&mut self) {
        let cell = Cell {
            timestamp_micros: self.timestamp_micros,
            value: std::mem::take(&mut self.value).freeze(),
            labels: std::mem::take(&mut self.labels),
        };
        if let Some(family) = self
            .row
            .families
            .iter_mut()
            .find(|family| family.name == self.family)
        {
            if let Some(column) = family
                .columns
                .iter_mut()
                .find(|column| column.qualifier == self.qualifier)
            {
                column.cells.push(cell);
            } else {
                family.columns.push(Column {
                    qualifier: self.qualifier.clone(),
                    cells: vec![cell],
                });
            }
        } else {
            self.row.families.push(Family {
                name: self.family.clone(),
                columns: vec![Column {
                    qualifier: self.qualifier.clone(),
                    cells: vec![cell],
                }],
            });
        }
    }

    fn commit_row(&mut self) -> Row {
        let row = std::mem::replace(&mut self.row, empty_row());
        self.last_complete_key = Some(row.key.clone());
        self.clear_cell();
        self.state = State::NewRow;
        row
    }

    fn validate_order(&self, key: &Bytes) -> Result<(), RowMergeIssue> {
        let Some(previous) = &self.last_complete_key else {
            return Ok(());
        };
        let ordered = if self.reversed {
            key < previous
        } else {
            key > previous
        };
        if ordered {
            Ok(())
        } else {
            Err(RowMergeIssue::OutOfOrderRowKey)
        }
    }

    fn validate_reset(chunk: &CellChunk) -> Result<(), RowMergeIssue> {
        let has_other_data = !chunk.row_key.is_empty()
            || chunk.family_name.is_some()
            || chunk.qualifier.is_some()
            || chunk.timestamp_micros != 0
            || !chunk.labels.is_empty()
            || !chunk.value.is_empty()
            || chunk.value_size != 0;
        if has_other_data {
            Err(RowMergeIssue::ResetWithData)
        } else {
            Ok(())
        }
    }

    fn clear_active_row(&mut self) {
        self.row = empty_row();
        self.family.clear();
        self.qualifier = Bytes::new();
        self.clear_cell();
        self.state = State::NewRow;
    }

    fn clear_cell(&mut self) {
        self.timestamp_micros = 0;
        self.labels.clear();
        self.value.clear();
        self.expected_value_size = 0;
        self.remaining_value_bytes = 0;
    }
}

fn empty_row() -> Row {
    Row {
        key: Bytes::new(),
        families: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::RowMerger;
    use crate::{
        RowMergeIssue,
        proto::read_rows_response::{CellChunk, cell_chunk::RowStatus},
    };

    fn chunk(
        row_key: &'static [u8],
        family: Option<&str>,
        qualifier: Option<&'static [u8]>,
        timestamp_micros: i64,
        value: &'static [u8],
        value_size: i32,
        status: Option<RowStatus>,
    ) -> CellChunk {
        CellChunk {
            row_key: Bytes::from_static(row_key),
            family_name: family.map(str::to_owned),
            qualifier: qualifier.map(<[u8]>::to_vec),
            timestamp_micros,
            labels: Vec::new(),
            value: Bytes::from_static(value),
            value_size,
            row_status: status,
        }
    }

    #[test]
    fn merges_families_columns_versions_labels_and_empty_values() {
        let mut merger = RowMerger::new(false);
        let mut first = chunk(b"row", Some("a"), Some(b"q"), 3, b"v3", 0, None);
        first.labels = vec!["matched".to_owned()];

        assert!(merger.push(&first).expect("first cell").is_none());
        assert!(
            merger
                .push(&chunk(b"", None, None, 2, b"v2", 0, None))
                .expect("second version")
                .is_none()
        );
        assert!(
            merger
                .push(&chunk(b"", Some("b"), Some(b""), 1, b"", 0, None))
                .expect("empty cell")
                .is_none()
        );
        let row = merger
            .push(&chunk(
                b"",
                None,
                None,
                0,
                b"",
                0,
                Some(RowStatus::CommitRow(true)),
            ))
            .expect("commit")
            .expect("complete row");

        assert_eq!(row.key.as_ref(), b"row");
        assert_eq!(row.families.len(), 2);
        assert_eq!(row.families[0].columns[0].cells.len(), 2);
        assert_eq!(row.families[0].columns[0].cells[0].labels, vec!["matched"]);
        assert_eq!(row.families[1].columns[0].qualifier.as_ref(), b"");
        assert_eq!(row.families[1].columns[0].cells.len(), 2);
        assert!(row.families[1].columns[0].cells[1].value.is_empty());
    }

    #[test]
    fn split_cell_can_cross_any_message_boundary() {
        let mut merger = RowMerger::new(false);

        assert!(
            merger
                .push(&chunk(b"row", Some("f"), Some(b"q"), 1, b"ab", 6, None))
                .expect("first split")
                .is_none()
        );
        assert!(
            merger
                .push(&chunk(b"", None, None, 0, b"cd", 6, None))
                .expect("middle split")
                .is_none()
        );
        let row = merger
            .push(&chunk(
                b"",
                None,
                None,
                0,
                b"ef",
                0,
                Some(RowStatus::CommitRow(true)),
            ))
            .expect("last split")
            .expect("complete row");

        assert_eq!(
            row.families[0].columns[0].cells[0].value.as_ref(),
            b"abcdef"
        );
        assert!(merger.finish().is_ok());
    }

    #[test]
    fn split_cell_can_end_with_an_empty_terminal_chunk() {
        let mut merger = RowMerger::new(false);
        assert!(
            merger
                .push(&chunk(b"row", Some("f"), Some(b"q"), 1, b"ab", 2, None))
                .expect("first split")
                .is_none()
        );
        let row = merger
            .push(&chunk(
                b"",
                None,
                None,
                0,
                b"",
                0,
                Some(RowStatus::CommitRow(true)),
            ))
            .expect("empty terminal chunk")
            .expect("complete row");

        assert_eq!(row.families[0].columns[0].cells[0].value.as_ref(), b"ab");
    }

    #[test]
    fn reset_discards_the_partial_row_and_allows_it_to_repeat() {
        let mut merger = RowMerger::new(false);

        merger
            .push(&chunk(b"row", Some("f"), Some(b"q"), 1, b"old", 0, None))
            .expect("partial row");
        merger
            .push(&chunk(
                b"",
                None,
                None,
                0,
                b"",
                0,
                Some(RowStatus::ResetRow(true)),
            ))
            .expect("reset");
        let row = merger
            .push(&chunk(
                b"row",
                Some("f"),
                Some(b"q"),
                2,
                b"new",
                0,
                Some(RowStatus::CommitRow(true)),
            ))
            .expect("replacement row")
            .expect("complete row");

        assert_eq!(row.families[0].columns[0].cells[0].value.as_ref(), b"new");
    }

    #[test]
    fn scan_markers_and_rows_must_follow_scan_order() {
        let mut forward = RowMerger::new(false);
        forward
            .scan_marker(Bytes::from_static(b"m"))
            .expect("forward marker");
        assert!(matches!(
            forward.push(&chunk(
                b"a",
                Some("f"),
                Some(b"q"),
                1,
                b"v",
                0,
                Some(RowStatus::CommitRow(true))
            )),
            Err(RowMergeIssue::OutOfOrderRowKey)
        ));

        let mut reverse = RowMerger::new(true);
        reverse
            .scan_marker(Bytes::from_static(b"m"))
            .expect("reverse marker");
        assert!(
            reverse
                .push(&chunk(
                    b"a",
                    Some("f"),
                    Some(b"q"),
                    1,
                    b"v",
                    0,
                    Some(RowStatus::CommitRow(true))
                ))
                .expect("descending row")
                .is_some()
        );
    }

    #[test]
    fn incomplete_stream_and_scan_marker_during_row_are_rejected() {
        let mut merger = RowMerger::new(false);
        merger
            .push(&chunk(b"row", Some("f"), Some(b"q"), 1, b"v", 0, None))
            .expect("partial row");

        assert_eq!(merger.finish(), Err(RowMergeIssue::IncompleteRow));
        assert_eq!(
            merger.scan_marker(Bytes::from_static(b"z")),
            Err(RowMergeIssue::ScanMarkerDuringRow)
        );
    }

    #[test]
    fn invalid_new_row_chunks_are_rejected() {
        let cases = [
            (
                chunk(b"", None, None, 0, b"", 0, Some(RowStatus::ResetRow(true))),
                RowMergeIssue::ResetBetweenRows,
            ),
            (
                chunk(b"", Some("f"), Some(b"q"), 1, b"v", 0, None),
                RowMergeIssue::MissingRowKey,
            ),
            (
                chunk(b"row", None, Some(b"q"), 1, b"v", 0, None),
                RowMergeIssue::MissingFamily,
            ),
            (
                chunk(b"row", Some("f"), None, 1, b"v", 0, None),
                RowMergeIssue::MissingQualifier,
            ),
        ];

        for (chunk, expected) in cases {
            assert_eq!(RowMerger::new(false).push(&chunk), Err(expected));
        }
    }

    #[test]
    fn invalid_cell_boundaries_are_rejected() {
        let mut changed_row = RowMerger::new(false);
        changed_row
            .push(&chunk(b"a", Some("f"), Some(b"q"), 1, b"v", 0, None))
            .expect("first cell");
        assert_eq!(
            changed_row.push(&chunk(b"b", None, None, 1, b"v", 0, None)),
            Err(RowMergeIssue::RowKeyChanged)
        );

        let mut family_without_qualifier = RowMerger::new(false);
        family_without_qualifier
            .push(&chunk(b"a", Some("f"), Some(b"q"), 1, b"v", 0, None))
            .expect("first cell");
        assert_eq!(
            family_without_qualifier.push(&chunk(b"", Some("g"), None, 1, b"v", 0, None)),
            Err(RowMergeIssue::FamilyWithoutQualifier)
        );
    }

    #[test]
    fn invalid_split_values_are_rejected() {
        let initial_cases = [
            (
                chunk(b"row", Some("f"), Some(b"q"), 1, b"", 4, None),
                RowMergeIssue::SplitValueMissingData,
            ),
            (
                chunk(b"row", Some("f"), Some(b"q"), 1, b"abcde", 4, None),
                RowMergeIssue::SplitValueTooLarge,
            ),
            (
                chunk(
                    b"row",
                    Some("f"),
                    Some(b"q"),
                    1,
                    b"a",
                    4,
                    Some(RowStatus::CommitRow(true)),
                ),
                RowMergeIssue::CommitBeforeCellComplete,
            ),
        ];
        for (chunk, expected) in initial_cases {
            assert_eq!(RowMerger::new(false).push(&chunk), Err(expected));
        }

        let continuation_cases = [
            (
                chunk(b"", None, Some(b"q"), 0, b"b", 4, None),
                RowMergeIssue::CellMetadataOnContinuation,
            ),
            (
                chunk(b"", None, None, 0, b"b", 5, None),
                RowMergeIssue::SplitValueSizeChanged,
            ),
            (
                chunk(b"", None, None, 0, b"bc", 0, None),
                RowMergeIssue::SplitValueWrongSize,
            ),
            (
                chunk(
                    b"",
                    None,
                    None,
                    0,
                    b"b",
                    4,
                    Some(RowStatus::CommitRow(true)),
                ),
                RowMergeIssue::CommitBeforeCellComplete,
            ),
        ];
        for (continuation, expected) in continuation_cases {
            let mut merger = RowMerger::new(false);
            merger
                .push(&chunk(b"row", Some("f"), Some(b"q"), 1, b"a", 4, None))
                .expect("first split");
            assert_eq!(merger.push(&continuation), Err(expected));
        }
    }

    #[test]
    fn reset_with_cell_data_is_rejected() {
        let mut merger = RowMerger::new(false);
        merger
            .push(&chunk(b"row", Some("f"), Some(b"q"), 1, b"a", 4, None))
            .expect("first split");

        assert_eq!(
            merger.push(&chunk(
                b"",
                None,
                None,
                0,
                b"x",
                0,
                Some(RowStatus::ResetRow(true))
            )),
            Err(RowMergeIssue::ResetWithData)
        );
    }

    #[test]
    fn retry_can_discard_only_the_partial_row() {
        let mut merger = RowMerger::new(false);
        assert!(
            merger
                .push(&chunk(
                    b"a",
                    Some("f"),
                    Some(b"q"),
                    1,
                    b"ok",
                    0,
                    Some(RowStatus::CommitRow(true))
                ))
                .expect("first row")
                .is_some()
        );
        merger
            .push(&chunk(b"b", Some("f"), Some(b"q"), 1, b"bad", 0, None))
            .expect("partial second row");
        merger.discard_partial();

        let row = merger
            .push(&chunk(
                b"b",
                Some("f"),
                Some(b"q"),
                2,
                b"retry",
                0,
                Some(RowStatus::CommitRow(true)),
            ))
            .expect("retried row")
            .expect("complete row");
        assert_eq!(row.families[0].columns[0].cells[0].value.as_ref(), b"retry");
    }
}
