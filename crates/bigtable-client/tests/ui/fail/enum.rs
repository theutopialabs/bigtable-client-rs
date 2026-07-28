use bigtable_client::FromRow;

#[derive(FromRow)]
enum Record {
    One,
}

fn main() {}
