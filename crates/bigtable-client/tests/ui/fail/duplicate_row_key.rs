use bigtable_client::FromRow;

#[derive(FromRow)]
struct Record {
    #[bigtable(row_key)]
    first: String,
    #[bigtable(row_key)]
    second: String,
}

fn main() {}
