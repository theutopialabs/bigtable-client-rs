use bigtable_client::FromRow;

#[derive(FromRow)]
#[bigtable(family = "profile")]
struct Record {
    #[bigtable(qualifier = 42)]
    value: String,
}

fn main() {}
