use bigtable_client::FromRow;

#[derive(FromRow)]
#[bigtable(family = "profile")]
struct Record {
    #[bigtable(default)]
    value: Option<String>,
}

fn main() {}
