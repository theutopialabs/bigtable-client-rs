use bigtable_client::FromRow;

#[derive(FromRow)]
struct Record {
    #[bigtable(row_key)]
    key: Option<String>,
}

fn main() {}
