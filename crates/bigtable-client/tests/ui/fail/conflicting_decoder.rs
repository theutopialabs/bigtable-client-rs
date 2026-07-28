use bigtable_client::FromRow;

#[derive(FromRow)]
#[bigtable(family = "profile")]
struct Record {
    #[bigtable(json, with = "decode")]
    value: String,
}

fn decode(value: &[u8]) -> Result<String, std::string::FromUtf8Error> {
    String::from_utf8(value.to_vec())
}

fn main() {}
