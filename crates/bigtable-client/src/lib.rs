//! An async Rust client for Google Cloud Bigtable.
//!
//! The high-level API provides row queries, streamed row assembly, retry
//! resumption, and separate attempt and operation deadlines. The generated
//! Tonic client remains available for direct data API calls.
//!
//! # Quick start
//!
//! ```no_run
//! use bigtable_client::{Client, ClientConfig, Query};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ClientConfig::new("my-project", "my-instance")?;
//! let client = Client::connect(config).await?;
//! let query = Query::new("events")?
//!     .prefix(b"user#".to_vec())
//!     .limit(100)?;
//! let mut rows = client.read_rows(query).await?;
//! while let Some(row) = rows.next().await {
//!     println!("{:?}", row?.key);
//! }
//! # Ok(())
//! # }
//! ```

mod auth;
mod channel;
mod client;
mod config;
mod error;
mod merge;
mod mutation;
mod query;
mod read;
mod resource;
mod retry;
mod row;
mod write;

pub use client::{AuthInterceptor, Client, RawClient};
pub use config::ClientConfig;
pub use error::{
    BulkMutationError, BulkMutationPolicyIssue, ConfigField, ConfigIssue, Error,
    MutateRowsResponseIssue, MutationFailure, MutationFailureCause, MutationIssue, QueryIssue,
    ReadPolicyIssue, RowMergeIssue,
};
pub use mutation::{BulkMutation, Mutation, RowMutation};
pub use query::{Query, RowBound, RowRange};
pub use read::RowStream;
pub use retry::{DeadlinePolicy, Jitter, ReadOptions, RetryPolicy};
pub use row::{Cell, Column, Family, Row};
pub use write::{BatchPolicy, BulkMutationOptions, BulkMutationResult};

/// Generated Google Cloud Bigtable v2 types and the raw Tonic client.
pub use googleapis_tonic_google_bigtable_v2::google::bigtable::v2 as proto;
