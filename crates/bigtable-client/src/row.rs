use bytes::Bytes;

/// A complete Bigtable row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Row {
    /// The row key.
    pub key: Bytes,
    /// Column families in server response order.
    pub families: Vec<Family>,
}

/// A column family in a row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Family {
    /// The family name.
    pub name: String,
    /// Columns in server response order.
    pub columns: Vec<Column>,
}

/// A column in a family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Column {
    /// The raw column qualifier.
    pub qualifier: Bytes,
    /// Cells in server response order.
    pub cells: Vec<Cell>,
}

/// A cell version in a column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Cell {
    /// The stored timestamp in microseconds.
    pub timestamp_micros: i64,
    /// The raw cell value.
    pub value: Bytes,
    /// Labels added by a row filter.
    pub labels: Vec<String>,
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{Cell, Column, Family, Row};

    #[test]
    fn row_model_keeps_binary_keys_qualifiers_and_values() {
        let row = Row {
            key: Bytes::from_static(b"\x00row"),
            families: vec![Family {
                name: "family".to_owned(),
                columns: vec![Column {
                    qualifier: Bytes::from_static(b"\xffqualifier"),
                    cells: vec![Cell {
                        timestamp_micros: 42,
                        value: Bytes::from_static(b"\x00\xffvalue"),
                        labels: vec!["matched".to_owned()],
                    }],
                }],
            }],
        };

        assert_eq!(row.key.as_ref(), b"\x00row");
        assert_eq!(row.families[0].name, "family");
        assert_eq!(
            row.families[0].columns[0].qualifier.as_ref(),
            b"\xffqualifier"
        );
        assert_eq!(
            row.families[0].columns[0].cells[0].value.as_ref(),
            b"\x00\xffvalue"
        );
    }
}
