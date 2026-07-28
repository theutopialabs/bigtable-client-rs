//! An async Rust client for Google Cloud Bigtable.

mod auth;
mod channel;
mod client;
mod config;
mod error;

pub use client::{AuthInterceptor, Client, RawClient};
pub use config::ClientConfig;
pub use error::{ConfigField, ConfigIssue, Error};

/// Generated Google Cloud Bigtable v2 types and the raw Tonic client.
pub use googleapis_tonic_google_bigtable_v2::google::bigtable::v2 as proto;
