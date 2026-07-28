//! An async Rust client for Google Cloud Bigtable.

mod config;
mod error;

pub use config::ClientConfig;
pub use error::{ConfigField, ConfigIssue, Error};
