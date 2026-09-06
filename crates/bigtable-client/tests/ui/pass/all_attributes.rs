use bigtable_client::{FromRow, Row};
use bytes::Bytes;
use serde::Deserialize;

mod custom {
    pub struct Option<T>(pub T);
}

impl<T> bigtable_client::FromCellValue for custom::Option<T>
where
    T: bigtable_client::FromCellValue,
{
    fn from_cell_value(value: &Bytes) -> Result<Self, bigtable_client::ValueDecodeError> {
        T::from_cell_value(value).map(Self)
    }
}

#[derive(Deserialize)]
struct Details {
    name: String,
}

#[derive(FromRow)]
#[bigtable(family = "profile")]
struct Record<T> {
    #[bigtable(row_key)]
    key: Bytes,
    value: T,
    optional: Option<String>,
    #[bigtable(default)]
    count: u64,
    #[bigtable(json)]
    details: Details,
    #[bigtable(family = "binary", qualifier = b"\xff", with = "decode")]
    custom: String,
    custom_option: custom::Option<String>,
}

fn decode(value: &[u8]) -> Result<String, std::string::FromUtf8Error> {
    String::from_utf8(value.to_vec())
}

fn assert_mapper<T>()
where
    T: bigtable_client::FromRow,
{
}

fn main() {
    assert_mapper::<Record<String>>();
    let _ = <Record<String> as bigtable_client::FromRow>::from_row
        as fn(Row) -> Result<Record<String>, bigtable_client::RowMappingError>;
}
