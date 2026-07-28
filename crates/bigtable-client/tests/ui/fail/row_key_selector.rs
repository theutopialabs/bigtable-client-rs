use bigtable_client::FromRow;

#[derive(FromRow)]
struct Record {
    #[bigtable(row_key, family = "profile")]
    key: String,
}

fn main() {}
