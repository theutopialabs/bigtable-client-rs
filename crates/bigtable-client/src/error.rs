use std::fmt;

use thiserror::Error;

/// A client operation error.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The client configuration contains an invalid value.
    #[error("invalid Bigtable configuration for {field}: {issue}")]
    InvalidConfig {
        /// The invalid field.
        field: ConfigField,
        /// Why the value is invalid.
        issue: ConfigIssue,
    },

    /// The environment configuration could not be loaded.
    #[error("failed to load Bigtable configuration: {source}")]
    ConfigLoad {
        /// The configuration source error.
        #[source]
        source: Box<figment::Error>,
    },

    /// Application default credentials could not provide an access token.
    #[error("failed to authenticate with Google Cloud: {source}")]
    Authentication {
        /// The authentication source error.
        #[source]
        source: Box<gcp_auth::Error>,
    },

    /// A Google Cloud access token could not be used as gRPC metadata.
    #[error("Google Cloud returned an invalid access token: {source}")]
    InvalidAccessToken {
        /// The metadata parsing error.
        #[source]
        source: tonic::metadata::errors::InvalidMetadataValue,
    },

    /// Static client metadata could not be encoded for a gRPC request.
    #[error("failed to encode the {header} metadata header: {source}")]
    InvalidClientMetadata {
        /// The header that failed.
        header: &'static str,
        /// The metadata parsing error.
        #[source]
        source: tonic::metadata::errors::InvalidMetadataValue,
    },

    /// The gRPC channel could not be built or connected.
    #[error("failed to connect to Bigtable: {source}")]
    Transport {
        /// The transport source error.
        #[source]
        source: Box<tonic::transport::Error>,
    },

    /// The internal channel pool stopped while it was being initialized.
    #[error("failed to initialize the Bigtable channel pool")]
    ChannelPoolClosed,
}

impl Error {
    pub(crate) const fn invalid_config(field: ConfigField, issue: ConfigIssue) -> Self {
        Self::InvalidConfig { field, issue }
    }
}

/// A field in [`crate::ClientConfig`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigField {
    /// The Google Cloud project ID.
    ProjectId,
    /// The Bigtable instance ID.
    InstanceId,
    /// The Bigtable application profile ID.
    AppProfileId,
    /// The Bigtable service endpoint.
    Endpoint,
    /// The Bigtable emulator endpoint.
    EmulatorHost,
    /// The number of gRPC channels.
    ChannelPoolSize,
    /// The channel connection timeout.
    ConnectTimeout,
    /// The default request timeout.
    RequestTimeout,
    /// The HTTP/2 keepalive interval.
    KeepAliveInterval,
    /// The HTTP/2 keepalive timeout.
    KeepAliveTimeout,
}

impl fmt::Display for ConfigField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ProjectId => "project_id",
            Self::InstanceId => "instance_id",
            Self::AppProfileId => "app_profile_id",
            Self::Endpoint => "endpoint",
            Self::EmulatorHost => "emulator_host",
            Self::ChannelPoolSize => "channel_pool_size",
            Self::ConnectTimeout => "connect_timeout",
            Self::RequestTimeout => "request_timeout",
            Self::KeepAliveInterval => "keep_alive_interval",
            Self::KeepAliveTimeout => "keep_alive_timeout",
        })
    }
}

/// The reason a configuration value is invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigIssue {
    /// The value is empty.
    Empty,
    /// The value is not a valid absolute HTTP or HTTPS URI.
    InvalidUri,
    /// The number must be greater than zero.
    MustBePositive,
}

impl fmt::Display for ConfigIssue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "must not be empty",
            Self::InvalidUri => "must be an absolute HTTP or HTTPS URI",
            Self::MustBePositive => "must be greater than zero",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfigField, ConfigIssue, Error};

    #[test]
    fn invalid_config_display_names_the_field_and_issue() {
        let error = Error::invalid_config(ConfigField::ProjectId, ConfigIssue::Empty);

        assert_eq!(
            error.to_string(),
            "invalid Bigtable configuration for project_id: must not be empty"
        );
    }

    #[test]
    fn config_load_display_includes_source() {
        let source = figment::Figment::new()
            .extract::<String>()
            .expect_err("empty config cannot produce a string");
        let source_message = source.to_string();
        let error = Error::ConfigLoad {
            source: Box::new(source),
        };

        assert_eq!(
            error.to_string(),
            format!("failed to load Bigtable configuration: {source_message}")
        );
    }

    #[test]
    fn invalid_access_token_display_includes_source() {
        let source = "bad\nvalue"
            .parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>()
            .expect_err("newlines are not valid metadata");
        let source_message = source.to_string();
        let error = Error::InvalidAccessToken { source };

        assert_eq!(
            error.to_string(),
            format!("Google Cloud returned an invalid access token: {source_message}")
        );
    }

    #[test]
    fn invalid_client_metadata_display_names_header_and_source() {
        let source = "bad\nvalue"
            .parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>()
            .expect_err("newlines are not valid metadata");
        let source_message = source.to_string();
        let error = Error::InvalidClientMetadata {
            header: "test-header",
            source,
        };

        assert_eq!(
            error.to_string(),
            format!("failed to encode the test-header metadata header: {source_message}")
        );
    }

    #[test]
    fn authentication_display_includes_source() {
        let error = Error::Authentication {
            source: Box::new(gcp_auth::Error::Str("test auth failure")),
        };

        assert_eq!(
            error.to_string(),
            "failed to authenticate with Google Cloud: test auth failure"
        );
    }

    #[test]
    fn channel_pool_error_has_recovery_context() {
        assert_eq!(
            Error::ChannelPoolClosed.to_string(),
            "failed to initialize the Bigtable channel pool"
        );
    }

    #[test]
    fn every_config_field_has_a_stable_name() {
        let cases = [
            (ConfigField::ProjectId, "project_id"),
            (ConfigField::InstanceId, "instance_id"),
            (ConfigField::AppProfileId, "app_profile_id"),
            (ConfigField::Endpoint, "endpoint"),
            (ConfigField::EmulatorHost, "emulator_host"),
            (ConfigField::ChannelPoolSize, "channel_pool_size"),
            (ConfigField::ConnectTimeout, "connect_timeout"),
            (ConfigField::RequestTimeout, "request_timeout"),
            (ConfigField::KeepAliveInterval, "keep_alive_interval"),
            (ConfigField::KeepAliveTimeout, "keep_alive_timeout"),
        ];

        for (field, expected) in cases {
            assert_eq!(field.to_string(), expected);
        }
    }

    #[test]
    fn every_config_issue_has_clear_guidance() {
        let cases = [
            (ConfigIssue::Empty, "must not be empty"),
            (
                ConfigIssue::InvalidUri,
                "must be an absolute HTTP or HTTPS URI",
            ),
            (ConfigIssue::MustBePositive, "must be greater than zero"),
        ];

        for (issue, expected) in cases {
            assert_eq!(issue.to_string(), expected);
        }
    }
}
