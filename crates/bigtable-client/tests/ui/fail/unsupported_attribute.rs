use bigtable_client::FromRow;

#[derive(FromRow)]
#[bigtable(family = "profile")]
struct Record {
    #[bigtable(flatten)]
    value: String,
}

fn main() {}
