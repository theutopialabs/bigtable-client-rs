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
