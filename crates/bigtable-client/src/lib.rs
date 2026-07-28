//! An async Rust client for Google Cloud Bigtable.
//!
//! M0 provides configuration, application default credentials, pooled Tonic
//! channels, emulator support, and access to the generated Bigtable client.
//!
//! # Quick start
//!
//! ```no_run
//! use bigtable_client::{Client, ClientConfig, proto::PingAndWarmRequest};
//! use tonic::Request;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ClientConfig::new("my-project", "my-instance")?;
//! let client = Client::connect(config).await?;
//! let mut raw = client.raw_client();
//!
//! let mut request = Request::new(PingAndWarmRequest {
//!     name: "projects/my-project/instances/my-instance".to_owned(),
//!     app_profile_id: "default".to_owned(),
//! });
//! request.metadata_mut().insert(
//!     "x-goog-request-params",
//!     "name=projects/my-project/instances/my-instance".parse()?,
//! );
//! raw.ping_and_warm(request).await?;
//! # Ok(())
//! # }
//! ```

mod auth;
mod channel;
mod client;
mod config;
mod error;
mod merge;
mod query;
mod read;
mod retry;
mod row;

pub use client::{AuthInterceptor, Client, RawClient};
pub use config::ClientConfig;
pub use error::{ConfigField, ConfigIssue, Error, QueryIssue, ReadPolicyIssue, RowMergeIssue};
pub use query::{Query, RowBound, RowRange};
pub use read::RowStream;
pub use retry::{DeadlinePolicy, Jitter, ReadOptions, RetryPolicy};
pub use row::{Cell, Column, Family, Row};

/// Generated Google Cloud Bigtable v2 types and the raw Tonic client.
pub use googleapis_tonic_google_bigtable_v2::google::bigtable::v2 as proto;
