use bigtable_client::FromRow;

#[derive(FromRow)]
struct Record(String);

fn main() {}
