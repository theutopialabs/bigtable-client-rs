use std::{
    fmt,
    marker::PhantomData,
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_core::Stream;
use serde::de::DeserializeOwned;

use crate::{
    Cell, Error, Row, RowMappingError, RowMappingIssue, RowStream, RowValueLocation,
    ValueDecodeError,
};

/// Maps one complete Bigtable row to an owned Rust value.
pub trait FromRow: Sized {
    /// Maps a complete row.
    ///
    /// # Errors
    ///
    /// Returns a typed schema or value error when the row cannot populate the
    /// target value.
    fn from_row(row: Row) -> Result<Self, RowMappingError>;
}

impl Row {
    /// Maps this complete row to an owned Rust value.
    ///
    /// # Errors
    ///
    /// Returns a typed schema or value error when the row cannot populate the
    /// target value.
    pub fn map<T>(self) -> Result<T, RowMappingError>
    where
        T: FromRow,
    {
        T::from_row(self)
    }
}

/// Decodes one row key or cell value.
pub trait FromCellValue: Sized {
    /// Decodes raw Bigtable bytes.
    ///
    /// # Errors
    ///
    /// Returns a value error when the bytes do not use the expected encoding.
    fn from_cell_value(value: &Bytes) -> Result<Self, ValueDecodeError>;
}

/// Decodes a JSON row key or cell value.
pub trait FromJsonValue: Sized {
    /// Decodes UTF-8 JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns a value error when the JSON cannot populate this type.
    fn from_json_value(value: &[u8]) -> Result<Self, ValueDecodeError>;
}

impl<T> FromJsonValue for T
where
    T: DeserializeOwned,
{
    fn from_json_value(value: &[u8]) -> Result<Self, ValueDecodeError> {
        serde_json::from_slice(value).map_err(ValueDecodeError::new::<Self, _>)
    }
}

impl FromCellValue for Bytes {
    fn from_cell_value(value: &Bytes) -> Result<Self, ValueDecodeError> {
        Ok(value.clone())
    }
}

impl FromCellValue for Vec<u8> {
    fn from_cell_value(value: &Bytes) -> Result<Self, ValueDecodeError> {
        Ok(value.to_vec())
    }
}

impl FromCellValue for String {
    fn from_cell_value(value: &Bytes) -> Result<Self, ValueDecodeError> {
        String::from_utf8(value.to_vec()).map_err(ValueDecodeError::new::<Self, _>)
    }
}

macro_rules! impl_text_value {
    ($($type:ty),+ $(,)?) => {
        $(
            impl FromCellValue for $type {
                fn from_cell_value(value: &Bytes) -> Result<Self, ValueDecodeError> {
                    let text = std::str::from_utf8(value)
                        .map_err(ValueDecodeError::new::<Self, _>)?;
                    Self::from_str(text).map_err(ValueDecodeError::new::<Self, _>)
                }
            }
        )+
    };
}

impl_text_value!(
    bool, i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64
);

/// One decoded cell version with its Bigtable metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedCell<T> {
    /// Cell timestamp in microseconds.
    pub timestamp_micros: i64,
    /// Decoded cell value.
    pub value: T,
    /// Labels added by a row filter.
    pub labels: Vec<String>,
}

/// A borrowed decoder for one complete Bigtable row.
#[derive(Clone)]
pub struct RowDecoder<'a> {
    row: &'a Row,
}

impl<'a> RowDecoder<'a> {
    /// Creates a decoder over one complete row.
    #[must_use]
    pub const fn new(row: &'a Row) -> Self {
        Self { row }
    }

    /// Decodes the row key.
    ///
    /// # Errors
    ///
    /// Returns [`RowMappingIssue::InvalidValue`] when decoding fails.
    pub fn row_key<T>(&self) -> Result<T, RowMappingError>
    where
        T: FromCellValue,
    {
        T::from_cell_value(&self.row.key)
            .map_err(|source| self.invalid_value(RowValueLocation::RowKey, source))
    }

    /// Decodes the row key as JSON.
    ///
    /// # Errors
    ///
    /// Returns [`RowMappingIssue::InvalidValue`] when JSON decoding fails.
    pub fn row_key_json<T>(&self) -> Result<T, RowMappingError>
    where
        T: FromJsonValue,
    {
        T::from_json_value(&self.row.key)
            .map_err(|source| self.invalid_value(RowValueLocation::RowKey, source))
    }

    /// Decodes the row key with a caller-provided function.
    ///
    /// # Errors
    ///
    /// Returns [`RowMappingIssue::InvalidValue`] when the decoder fails.
    pub fn row_key_with<T, E, F>(&self, decode: F) -> Result<T, RowMappingError>
    where
        E: std::error::Error + Send + Sync + 'static,
        F: FnOnce(&[u8]) -> Result<T, E>,
    {
        decode(&self.row.key).map_err(|source| {
            self.invalid_value(
                RowValueLocation::RowKey,
                ValueDecodeError::new::<T, _>(source),
            )
        })
    }

    /// Decodes the latest visible cell for a required column.
    ///
    /// # Errors
    ///
    /// Returns a missing schema error or an invalid value error.
    pub fn required<T>(
        &self,
        family: &str,
        qualifier: impl AsRef<[u8]>,
    ) -> Result<T, RowMappingError>
    where
        T: FromCellValue,
    {
        let qualifier = qualifier.as_ref();
        let cell = self.latest_cell(family, qualifier)?;
        self.decode_cell(family, qualifier, cell)
    }

    /// Decodes the latest visible cell when a sparse column is present.
    ///
    /// # Errors
    ///
    /// Returns an invalid value error when the present cell cannot be decoded.
    pub fn optional<T>(
        &self,
        family: &str,
        qualifier: impl AsRef<[u8]>,
    ) -> Result<Option<T>, RowMappingError>
    where
        T: FromCellValue,
    {
        let qualifier = qualifier.as_ref();
        self.optional_latest_cell(family, qualifier)
            .map(|cell| self.decode_cell(family, qualifier, cell))
            .transpose()
    }

    /// Decodes required JSON from the latest visible cell.
    ///
    /// # Errors
    ///
    /// Returns a missing schema error or an invalid JSON error.
    pub fn required_json<T>(
        &self,
        family: &str,
        qualifier: impl AsRef<[u8]>,
    ) -> Result<T, RowMappingError>
    where
        T: FromJsonValue,
    {
        let qualifier = qualifier.as_ref();
        let cell = self.latest_cell(family, qualifier)?;
        self.decode_json_cell(family, qualifier, cell)
    }

    /// Decodes optional JSON from the latest visible cell.
    ///
    /// # Errors
    ///
    /// Returns an invalid JSON error when the present cell cannot be decoded.
    pub fn optional_json<T>(
        &self,
        family: &str,
        qualifier: impl AsRef<[u8]>,
    ) -> Result<Option<T>, RowMappingError>
    where
        T: FromJsonValue,
    {
        let qualifier = qualifier.as_ref();
        self.optional_latest_cell(family, qualifier)
            .map(|cell| self.decode_json_cell(family, qualifier, cell))
            .transpose()
    }

    /// Decodes a required cell with a caller-provided function.
    ///
    /// # Errors
    ///
    /// Returns a missing schema error or the wrapped decoder error.
    pub fn required_with<T, E, F>(
        &self,
        family: &str,
        qualifier: impl AsRef<[u8]>,
        decode: F,
    ) -> Result<T, RowMappingError>
    where
        E: std::error::Error + Send + Sync + 'static,
        F: FnOnce(&[u8]) -> Result<T, E>,
    {
        let qualifier = qualifier.as_ref();
        let cell = self.latest_cell(family, qualifier)?;
        self.decode_cell_with(family, qualifier, cell, decode)
    }

    /// Decodes an optional cell with a caller-provided function.
    ///
    /// # Errors
    ///
    /// Returns the wrapped decoder error when the present cell cannot be
    /// decoded.
    pub fn optional_with<T, E, F>(
        &self,
        family: &str,
        qualifier: impl AsRef<[u8]>,
        decode: F,
    ) -> Result<Option<T>, RowMappingError>
    where
        E: std::error::Error + Send + Sync + 'static,
        F: FnOnce(&[u8]) -> Result<T, E>,
    {
        let qualifier = qualifier.as_ref();
        self.optional_latest_cell(family, qualifier)
            .map(|cell| self.decode_cell_with(family, qualifier, cell, decode))
            .transpose()
    }

    /// Returns the latest visible cell for a required column.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the family, column, or cell is missing.
    pub fn latest_cell(
        &self,
        family: &str,
        qualifier: impl AsRef<[u8]>,
    ) -> Result<&'a Cell, RowMappingError> {
        let qualifier = qualifier.as_ref();
        let family_value = self.row.families.iter().find(|item| item.name == family);
        let Some(family_value) = family_value else {
            return Err(self.error(RowMappingIssue::MissingFamily {
                family: family.to_owned(),
            }));
        };
        let column = family_value
            .columns
            .iter()
            .find(|column| column.qualifier.as_ref() == qualifier);
        let Some(column) = column else {
            return Err(self.error(RowMappingIssue::MissingColumn {
                family: family.to_owned(),
                qualifier: Bytes::copy_from_slice(qualifier),
            }));
        };
        column.cells.first().ok_or_else(|| {
            self.error(RowMappingIssue::MissingCell {
                family: family.to_owned(),
                qualifier: Bytes::copy_from_slice(qualifier),
            })
        })
    }

    /// Returns the latest visible cell when a sparse column is present.
    #[must_use]
    pub fn optional_latest_cell(
        &self,
        family: &str,
        qualifier: impl AsRef<[u8]>,
    ) -> Option<&'a Cell> {
        let qualifier = qualifier.as_ref();
        self.row
            .families
            .iter()
            .find(|item| item.name == family)
            .and_then(|family| {
                family
                    .columns
                    .iter()
                    .find(|column| column.qualifier.as_ref() == qualifier)
            })
            .and_then(|column| column.cells.first())
    }

    /// Decodes every visible version for a required column.
    ///
    /// Versions stay in the decreasing timestamp order returned by Bigtable.
    ///
    /// # Errors
    ///
    /// Returns a missing schema error or the first invalid value error.
    pub fn versions<T>(
        &self,
        family: &str,
        qualifier: impl AsRef<[u8]>,
    ) -> Result<Vec<DecodedCell<T>>, RowMappingError>
    where
        T: FromCellValue,
    {
        let qualifier = qualifier.as_ref();
        self.cells(family, qualifier)?
            .iter()
            .map(|cell| {
                self.decode_cell(family, qualifier, cell)
                    .map(|value| DecodedCell {
                        timestamp_micros: cell.timestamp_micros,
                        value,
                        labels: cell.labels.clone(),
                    })
            })
            .collect()
    }

    /// Returns every visible raw cell for a required column.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the family, column, or cell is missing.
    pub fn cells(
        &self,
        family: &str,
        qualifier: impl AsRef<[u8]>,
    ) -> Result<&'a [Cell], RowMappingError> {
        let qualifier = qualifier.as_ref();
        let family_value = self.row.families.iter().find(|item| item.name == family);
        let Some(family_value) = family_value else {
            return Err(self.error(RowMappingIssue::MissingFamily {
                family: family.to_owned(),
            }));
        };
        let column = family_value
            .columns
            .iter()
            .find(|column| column.qualifier.as_ref() == qualifier);
        let Some(column) = column else {
            return Err(self.error(RowMappingIssue::MissingColumn {
                family: family.to_owned(),
                qualifier: Bytes::copy_from_slice(qualifier),
            }));
        };
        if column.cells.is_empty() {
            return Err(self.error(RowMappingIssue::MissingCell {
                family: family.to_owned(),
                qualifier: Bytes::copy_from_slice(qualifier),
            }));
        }
        Ok(&column.cells)
    }

    fn decode_cell<T>(
        &self,
        family: &str,
        qualifier: &[u8],
        cell: &Cell,
    ) -> Result<T, RowMappingError>
    where
        T: FromCellValue,
    {
        T::from_cell_value(&cell.value).map_err(|source| {
            self.invalid_value(
                RowValueLocation::Cell {
                    family: family.to_owned(),
                    qualifier: Bytes::copy_from_slice(qualifier),
                    timestamp_micros: cell.timestamp_micros,
                },
                source,
            )
        })
    }

    fn decode_json_cell<T>(
        &self,
        family: &str,
        qualifier: &[u8],
        cell: &Cell,
    ) -> Result<T, RowMappingError>
    where
        T: FromJsonValue,
    {
        T::from_json_value(&cell.value).map_err(|source| {
            self.invalid_value(
                RowValueLocation::Cell {
                    family: family.to_owned(),
                    qualifier: Bytes::copy_from_slice(qualifier),
                    timestamp_micros: cell.timestamp_micros,
                },
                source,
            )
        })
    }

    fn decode_cell_with<T, E, F>(
        &self,
        family: &str,
        qualifier: &[u8],
        cell: &Cell,
        decode: F,
    ) -> Result<T, RowMappingError>
    where
        E: std::error::Error + Send + Sync + 'static,
        F: FnOnce(&[u8]) -> Result<T, E>,
    {
        decode(&cell.value).map_err(|source| {
            self.invalid_value(
                RowValueLocation::Cell {
                    family: family.to_owned(),
                    qualifier: Bytes::copy_from_slice(qualifier),
                    timestamp_micros: cell.timestamp_micros,
                },
                ValueDecodeError::new::<T, _>(source),
            )
        })
    }

    fn invalid_value(
        &self,
        location: RowValueLocation,
        source: ValueDecodeError,
    ) -> RowMappingError {
        self.error(RowMappingIssue::InvalidValue { location, source })
    }

    fn error(&self, issue: RowMappingIssue) -> RowMappingError {
        RowMappingError::new(self.row.key.clone(), issue)
    }
}

impl fmt::Debug for RowDecoder<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RowDecoder")
            .field("row_key_bytes", &self.row.key.len())
            .finish_non_exhaustive()
    }
}

/// A stream that maps complete Bigtable rows to owned Rust values.
pub struct TypedRowStream<T> {
    inner: RowStream,
    target: PhantomData<fn() -> T>,
}

impl<T> TypedRowStream<T> {
    pub(crate) const fn new(inner: RowStream) -> Self {
        Self {
            inner,
            target: PhantomData,
        }
    }

    /// Returns the next mapped row.
    pub async fn next(&mut self) -> Option<Result<T, Error>>
    where
        T: FromRow,
    {
        self.inner.next().await.map(map_row::<T>)
    }
}

impl<T> Stream for TypedRowStream<T>
where
    T: FromRow,
{
    type Item = Result<T, Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner)
            .poll_next(context)
            .map(|item| item.map(map_row::<T>))
    }
}

impl<T> fmt::Debug for TypedRowStream<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TypedRowStream")
            .field("target", &std::any::type_name::<T>())
            .finish_non_exhaustive()
    }
}

fn map_row<T>(row: Result<Row, Error>) -> Result<T, Error>
where
    T: FromRow,
{
    row.and_then(|row| T::from_row(row).map_err(Error::from))
}

#[cfg(test)]
mod tests {
    use std::num::ParseIntError;

    use bytes::Bytes;
    use serde::Deserialize;

    use super::{DecodedCell, FromCellValue, FromRow, RowDecoder};
    use crate::{
        Cell, Column, Error, Family, Row, RowMappingIssue, RowValueLocation, ValueDecodeError,
    };

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    struct Profile {
        enabled: bool,
        count: u32,
    }

    #[derive(Debug, Eq, PartialEq)]
    struct ManualRow {
        key: String,
        name: String,
        profile: Profile,
        score: u32,
    }

    impl FromRow for ManualRow {
        fn from_row(row: Row) -> Result<Self, crate::RowMappingError> {
            let decoder = RowDecoder::new(&row);
            Ok(Self {
                key: decoder.row_key()?,
                name: decoder.required("profile", b"name")?,
                profile: decoder.required_json("profile", b"json")?,
                score: decoder.required_with("metrics", b"score", parse_hex)?,
            })
        }
    }

    fn parse_hex(value: &[u8]) -> Result<u32, ParseIntError> {
        u32::from_str_radix(std::str::from_utf8(value).unwrap_or(""), 16)
    }

    fn row() -> Row {
        Row {
            key: Bytes::from_static(b"user#1"),
            families: vec![
                Family {
                    name: "profile".to_owned(),
                    columns: vec![
                        Column {
                            qualifier: Bytes::from_static(b"name"),
                            cells: vec![
                                Cell {
                                    timestamp_micros: 3_000,
                                    value: Bytes::from_static(b"Ada"),
                                    labels: vec!["latest".to_owned()],
                                },
                                Cell {
                                    timestamp_micros: 2_000,
                                    value: Bytes::from_static(b"Augusta"),
                                    labels: Vec::new(),
                                },
                            ],
                        },
                        Column {
                            qualifier: Bytes::from_static(b"json"),
                            cells: vec![Cell {
                                timestamp_micros: 1_000,
                                value: Bytes::from_static(b"{\"enabled\":true,\"count\":7}"),
                                labels: Vec::new(),
                            }],
                        },
                    ],
                },
                Family {
                    name: "metrics".to_owned(),
                    columns: vec![Column {
                        qualifier: Bytes::from_static(b"score"),
                        cells: vec![Cell {
                            timestamp_micros: 4_000,
                            value: Bytes::from_static(b"ff"),
                            labels: Vec::new(),
                        }],
                    }],
                },
            ],
        }
    }

    #[test]
    fn common_value_decoders_cover_owned_bytes_text_booleans_and_numbers() {
        let value = Bytes::from_static(b"42");

        assert_eq!(Bytes::from_cell_value(&value).expect("bytes"), value);
        assert_eq!(
            Vec::<u8>::from_cell_value(&value).expect("byte vector"),
            b"42"
        );
        assert_eq!(String::from_cell_value(&value).expect("UTF-8 string"), "42");
        assert_eq!(u8::from_cell_value(&value).expect("u8"), 42);
        assert_eq!(u16::from_cell_value(&value).expect("u16"), 42);
        assert_eq!(u32::from_cell_value(&value).expect("u32"), 42);
        assert_eq!(u64::from_cell_value(&value).expect("u64"), 42);
        assert_eq!(u128::from_cell_value(&value).expect("u128"), 42);
        assert_eq!(usize::from_cell_value(&value).expect("usize"), 42);
        assert_eq!(i8::from_cell_value(&value).expect("i8"), 42);
        assert_eq!(i16::from_cell_value(&value).expect("i16"), 42);
        assert_eq!(i32::from_cell_value(&value).expect("i32"), 42);
        assert_eq!(i64::from_cell_value(&value).expect("i64"), 42);
        assert_eq!(i128::from_cell_value(&value).expect("i128"), 42);
        assert_eq!(isize::from_cell_value(&value).expect("isize"), 42);
        assert!((f32::from_cell_value(&value).expect("f32") - 42.0).abs() < f32::EPSILON);
        assert!((f64::from_cell_value(&value).expect("f64") - 42.0).abs() < f64::EPSILON);
        assert!(bool::from_cell_value(&Bytes::from_static(b"true")).expect("bool"));
    }

    #[test]
    fn value_decoders_preserve_the_target_and_source_error() {
        let utf8_error =
            String::from_cell_value(&Bytes::from_static(b"\xff")).expect_err("invalid UTF-8");
        let number_error =
            u32::from_cell_value(&Bytes::from_static(b"many")).expect_err("invalid number");

        assert_eq!(utf8_error.target(), "alloc::string::String");
        assert!(std::error::Error::source(&utf8_error).is_some());
        assert_eq!(number_error.target(), "u32");
        assert!(number_error.to_string().contains("invalid digit"));
    }

    #[test]
    fn decoder_maps_keys_latest_cells_json_custom_values_and_sparse_columns() {
        let row = row();
        let decoder = RowDecoder::new(&row);

        assert_eq!(decoder.row_key::<String>().expect("row key"), "user#1");
        assert_eq!(
            decoder
                .required::<String>("profile", b"name")
                .expect("name"),
            "Ada"
        );
        assert_eq!(
            decoder
                .optional::<String>("profile", b"nickname")
                .expect("sparse column"),
            None
        );
        assert_eq!(
            decoder
                .required_json::<Profile>("profile", b"json")
                .expect("profile JSON"),
            Profile {
                enabled: true,
                count: 7,
            }
        );
        assert_eq!(
            decoder
                .required_with("metrics", b"score", parse_hex)
                .expect("hex score"),
            255
        );
        assert_eq!(
            decoder
                .optional_with("metrics", b"missing", parse_hex)
                .expect("sparse custom value"),
            None
        );
    }

    #[test]
    fn decoder_preserves_every_cell_version_and_metadata() {
        let row = row();
        let decoder = RowDecoder::new(&row);

        assert_eq!(
            decoder
                .versions::<String>("profile", b"name")
                .expect("versions"),
            vec![
                DecodedCell {
                    timestamp_micros: 3_000,
                    value: "Ada".to_owned(),
                    labels: vec!["latest".to_owned()],
                },
                DecodedCell {
                    timestamp_micros: 2_000,
                    value: "Augusta".to_owned(),
                    labels: Vec::new(),
                },
            ]
        );
        assert_eq!(
            decoder
                .cells("profile", b"name")
                .expect("raw versions")
                .len(),
            2
        );
        assert_eq!(
            decoder
                .latest_cell("profile", b"name")
                .expect("latest")
                .timestamp_micros,
            3_000
        );
    }

    #[test]
    fn missing_family_column_and_cell_are_distinct() {
        let mut row = row();
        let missing_family = RowDecoder::new(&row)
            .required::<String>("missing", b"name")
            .expect_err("family is required");
        let missing_column = RowDecoder::new(&row)
            .required::<String>("profile", b"missing")
            .expect_err("column is required");
        row.families[0].columns[0].cells.clear();
        let missing_cell = RowDecoder::new(&row)
            .required::<String>("profile", b"name")
            .expect_err("cell is required");

        assert!(matches!(
            missing_family.issue(),
            RowMappingIssue::MissingFamily { family } if family == "missing"
        ));
        assert!(matches!(
            missing_column.issue(),
            RowMappingIssue::MissingColumn { family, qualifier }
                if family == "profile" && qualifier.as_ref() == b"missing"
        ));
        assert!(matches!(
            missing_cell.issue(),
            RowMappingIssue::MissingCell { family, qualifier }
                if family == "profile" && qualifier.as_ref() == b"name"
        ));
        assert_eq!(missing_cell.row_key().as_ref(), b"user#1");
    }

    #[test]
    fn invalid_key_cell_json_and_custom_values_keep_their_location() {
        let invalid_key = Row {
            key: Bytes::from_static(b"\xff"),
            ..row()
        };
        let key_error = RowDecoder::new(&invalid_key)
            .row_key::<String>()
            .expect_err("key is not UTF-8");
        let mut invalid_cell = row();
        invalid_cell.families[0].columns[0].cells[0].value = Bytes::from_static(b"\xff");
        let cell_error = RowDecoder::new(&invalid_cell)
            .required::<String>("profile", b"name")
            .expect_err("cell is not UTF-8");
        let json_error = RowDecoder::new(&invalid_cell)
            .required_json::<Profile>("profile", b"name")
            .expect_err("cell is not JSON");
        let custom_error = RowDecoder::new(&invalid_cell)
            .required_with("profile", b"name", parse_hex)
            .expect_err("cell is not hex");

        assert!(matches!(
            key_error.issue(),
            RowMappingIssue::InvalidValue {
                location: RowValueLocation::RowKey,
                ..
            }
        ));
        for error in [&cell_error, &json_error, &custom_error] {
            assert!(matches!(
                error.issue(),
                RowMappingIssue::InvalidValue {
                    location: RowValueLocation::Cell {
                        family,
                        qualifier,
                        timestamp_micros: 3_000,
                    },
                    ..
                } if family == "profile" && qualifier.as_ref() == b"name"
            ));
        }
    }

    #[test]
    fn manual_from_row_and_row_map_use_the_same_decoder_contract() {
        let expected = ManualRow {
            key: "user#1".to_owned(),
            name: "Ada".to_owned(),
            profile: Profile {
                enabled: true,
                count: 7,
            },
            score: 255,
        };

        assert_eq!(ManualRow::from_row(row()).expect("manual mapper"), expected);
        assert_eq!(row().map::<ManualRow>().expect("row mapper"), expected);
    }

    #[test]
    fn mapping_errors_convert_to_the_crate_error() {
        let mapping = RowDecoder::new(&row())
            .required::<String>("missing", b"name")
            .expect_err("family is required");
        let error = Error::from(mapping);

        assert!(matches!(error, Error::RowMapping(_)));
    }

    #[test]
    fn custom_value_errors_record_the_requested_target() {
        let source = "x".parse::<u32>().expect_err("invalid integer");
        let error = ValueDecodeError::new::<u32, _>(source);

        assert_eq!(error.target(), "u32");
    }
}
