use bigtable_client::FromRow;

#[derive(FromRow)]
struct Record {
    value: String,
}

fn main() {}
