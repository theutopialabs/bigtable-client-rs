use std::time::Duration;

use figment::{
    Figment,
    providers::{Env, Serialized},
};
use http::Uri;
use serde::{Deserialize, Serialize};

use crate::{ConfigField, ConfigIssue, Error};

const DEFAULT_APP_PROFILE_ID: &str = "default";
const MAX_APP_PROFILE_ID_CHARS: usize = 50;
const DEFAULT_ENDPOINT: &str = "https://bigtable.googleapis.com";
const DEFAULT_CHANNEL_POOL_SIZE: usize = 1;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
const DEFAULT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// Connection settings for a Bigtable client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientConfig {
    project_id: String,
    instance_id: String,
    app_profile_id: String,
    endpoint: String,
    emulator_host: Option<String>,
    channel_pool_size: usize,
    connect_timeout: Duration,
    request_timeout: Duration,
    keep_alive_interval: Duration,
    keep_alive_timeout: Duration,
}

impl ClientConfig {
    /// Creates a configuration with production-ready defaults.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when either ID is empty.
    pub fn new(
        project_id: impl Into<String>,
        instance_id: impl Into<String>,
    ) -> Result<Self, Error> {
        RawConfig {
            project_id: project_id.into(),
            instance_id: instance_id.into(),
            ..RawConfig::default()
        }
        .try_into()
    }

    /// Loads configuration from `BIGTABLE_` environment variables.
    ///
    /// `BIGTABLE_PROJECT_ID` and `BIGTABLE_INSTANCE_ID` are required. Durations
    /// accept values such as `500ms`, `10s`, and `2m`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigLoad`] when a value cannot be parsed. Returns
    /// [`Error::InvalidConfig`] when a parsed value is not usable.
    pub fn load() -> Result<Self, Error> {
        let raw = Figment::from(Serialized::defaults(RawConfig::default()))
            .merge(Env::prefixed("BIGTABLE_"))
            .extract::<RawConfig>()
            .map_err(|source| Error::ConfigLoad {
                source: Box::new(source),
            })?;

        raw.try_into()
    }

    /// Sets the application profile ID.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when the ID is empty or longer than 50
    /// characters.
    pub fn with_app_profile_id(mut self, app_profile_id: impl Into<String>) -> Result<Self, Error> {
        self.app_profile_id = non_empty_with_max_chars(
            &app_profile_id.into(),
            ConfigField::AppProfileId,
            MAX_APP_PROFILE_ID_CHARS,
        )?;
        Ok(self)
    }

    /// Sets the production service endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when the endpoint is not an absolute
    /// HTTP or HTTPS URI.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Result<Self, Error> {
        self.endpoint = valid_uri(&endpoint.into(), ConfigField::Endpoint)?;
        Ok(self)
    }

    /// Connects to a Bigtable emulator without authentication.
    ///
    /// A missing scheme is treated as `http://`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when the host is not an absolute HTTP
    /// or HTTPS URI.
    pub fn with_emulator_host(mut self, emulator_host: impl Into<String>) -> Result<Self, Error> {
        let host = emulator_host.into();
        let normalized = if host.contains("://") {
            host
        } else {
            format!("http://{host}")
        };
        self.emulator_host = Some(valid_uri(&normalized, ConfigField::EmulatorHost)?);
        Ok(self)
    }

    /// Sets the number of gRPC channels.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when the size is zero.
    pub fn with_channel_pool_size(mut self, channel_pool_size: usize) -> Result<Self, Error> {
        ensure_positive(channel_pool_size, ConfigField::ChannelPoolSize)?;
        self.channel_pool_size = channel_pool_size;
        Ok(self)
    }

    /// Sets the channel connection timeout.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when the timeout is zero.
    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Result<Self, Error> {
        ensure_nonzero(connect_timeout, ConfigField::ConnectTimeout)?;
        self.connect_timeout = connect_timeout;
        Ok(self)
    }

    /// Sets the default high-level operation timeout.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when the timeout is zero.
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Result<Self, Error> {
        ensure_nonzero(request_timeout, ConfigField::RequestTimeout)?;
        self.request_timeout = request_timeout;
        Ok(self)
    }

    /// Sets the HTTP/2 keepalive timing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when either duration is zero.
    pub fn with_keep_alive(mut self, interval: Duration, timeout: Duration) -> Result<Self, Error> {
        ensure_nonzero(interval, ConfigField::KeepAliveInterval)?;
        ensure_nonzero(timeout, ConfigField::KeepAliveTimeout)?;
        self.keep_alive_interval = interval;
        self.keep_alive_timeout = timeout;
        Ok(self)
    }

    /// Returns the Google Cloud project ID.
    #[must_use]
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Returns the Bigtable instance ID.
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Returns the application profile ID.
    #[must_use]
    pub fn app_profile_id(&self) -> &str {
        &self.app_profile_id
    }

    /// Returns the production service endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Returns the emulator endpoint when one is configured.
    #[must_use]
    pub fn emulator_host(&self) -> Option<&str> {
        self.emulator_host.as_deref()
    }

    /// Returns the number of gRPC channels.
    #[must_use]
    pub const fn channel_pool_size(&self) -> usize {
        self.channel_pool_size
    }

    /// Returns the channel connection timeout.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the default high-level operation timeout.
    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns the HTTP/2 keepalive interval.
    #[must_use]
    pub const fn keep_alive_interval(&self) -> Duration {
        self.keep_alive_interval
    }

    /// Returns the HTTP/2 keepalive timeout.
    #[must_use]
    pub const fn keep_alive_timeout(&self) -> Duration {
        self.keep_alive_timeout
    }

    /// Returns the endpoint used for new gRPC channels.
    #[must_use]
    pub fn service_endpoint(&self) -> &str {
        self.emulator_host.as_deref().unwrap_or(&self.endpoint)
    }

    /// Returns whether the client is configured for an emulator.
    #[must_use]
    pub const fn uses_emulator(&self) -> bool {
        self.emulator_host.is_some()
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct RawConfig {
    project_id: String,
    instance_id: String,
    app_profile_id: String,
    endpoint: String,
    emulator_host: Option<String>,
    channel_pool_size: usize,
    #[serde(with = "humantime_serde")]
    connect_timeout: Duration,
    #[serde(with = "humantime_serde")]
    request_timeout: Duration,
    #[serde(with = "humantime_serde")]
    keep_alive_interval: Duration,
    #[serde(with = "humantime_serde")]
    keep_alive_timeout: Duration,
}

impl Default for RawConfig {
    fn default() -> Self {
        Self {
            project_id: String::new(),
            instance_id: String::new(),
            app_profile_id: DEFAULT_APP_PROFILE_ID.to_owned(),
            endpoint: DEFAULT_ENDPOINT.to_owned(),
            emulator_host: None,
            channel_pool_size: DEFAULT_CHANNEL_POOL_SIZE,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL,
            keep_alive_timeout: DEFAULT_KEEP_ALIVE_TIMEOUT,
        }
    }
}

impl TryFrom<RawConfig> for ClientConfig {
    type Error = Error;

    fn try_from(raw: RawConfig) -> Result<Self, Self::Error> {
        let project_id = non_empty(&raw.project_id, ConfigField::ProjectId)?;
        let instance_id = non_empty(&raw.instance_id, ConfigField::InstanceId)?;
        let app_profile_id = non_empty_with_max_chars(
            &raw.app_profile_id,
            ConfigField::AppProfileId,
            MAX_APP_PROFILE_ID_CHARS,
        )?;
        let endpoint = valid_uri(&raw.endpoint, ConfigField::Endpoint)?;
        let emulator_host = raw
            .emulator_host
            .map(|host| {
                let normalized = if host.contains("://") {
                    host
                } else {
                    format!("http://{host}")
                };
                valid_uri(&normalized, ConfigField::EmulatorHost)
            })
            .transpose()?;
        ensure_positive(raw.channel_pool_size, ConfigField::ChannelPoolSize)?;
        ensure_nonzero(raw.connect_timeout, ConfigField::ConnectTimeout)?;
        ensure_nonzero(raw.request_timeout, ConfigField::RequestTimeout)?;
        ensure_nonzero(raw.keep_alive_interval, ConfigField::KeepAliveInterval)?;
        ensure_nonzero(raw.keep_alive_timeout, ConfigField::KeepAliveTimeout)?;

        Ok(Self {
            project_id,
            instance_id,
            app_profile_id,
            endpoint,
            emulator_host,
            channel_pool_size: raw.channel_pool_size,
            connect_timeout: raw.connect_timeout,
            request_timeout: raw.request_timeout,
            keep_alive_interval: raw.keep_alive_interval,
            keep_alive_timeout: raw.keep_alive_timeout,
        })
    }
}

fn non_empty(value: &str, field: ConfigField) -> Result<String, Error> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Error::invalid_config(field, ConfigIssue::Empty));
    }
    Ok(trimmed.to_owned())
}

fn non_empty_with_max_chars(
    value: &str,
    field: ConfigField,
    max_chars: usize,
) -> Result<String, Error> {
    let value = non_empty(value, field)?;
    if value.chars().count() > max_chars {
        return Err(Error::invalid_config(
            field,
            ConfigIssue::TooLong { max_chars },
        ));
    }
    Ok(value)
}

fn valid_uri(value: &str, field: ConfigField) -> Result<String, Error> {
    let value = non_empty(value, field)?;
    let uri = value
        .parse::<Uri>()
        .map_err(|_| Error::invalid_config(field, ConfigIssue::InvalidUri))?;
    let valid_scheme = matches!(uri.scheme_str(), Some("http" | "https"));
    let valid_path = matches!(uri.path(), "" | "/");
    if !valid_scheme || uri.authority().is_none() || !valid_path || uri.query().is_some() {
        return Err(Error::invalid_config(field, ConfigIssue::InvalidUri));
    }
    Ok(value)
}

fn ensure_positive(value: usize, field: ConfigField) -> Result<(), Error> {
    if value == 0 {
        return Err(Error::invalid_config(field, ConfigIssue::MustBePositive));
    }
    Ok(())
}

fn ensure_nonzero(value: Duration, field: ConfigField) -> Result<(), Error> {
    if value.is_zero() {
        return Err(Error::invalid_config(field, ConfigIssue::MustBePositive));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use figment::Jail;

    use super::{
        ClientConfig, DEFAULT_APP_PROFILE_ID, DEFAULT_CHANNEL_POOL_SIZE, DEFAULT_CONNECT_TIMEOUT,
        DEFAULT_ENDPOINT, DEFAULT_KEEP_ALIVE_INTERVAL, DEFAULT_KEEP_ALIVE_TIMEOUT,
        DEFAULT_REQUEST_TIMEOUT,
    };
    use crate::{ConfigField, ConfigIssue, Error};

    #[test]
    fn new_uses_production_defaults() {
        let config = ClientConfig::new("project", "instance").expect("valid config");

        assert_eq!(config.project_id(), "project");
        assert_eq!(config.instance_id(), "instance");
        assert_eq!(config.app_profile_id(), DEFAULT_APP_PROFILE_ID);
        assert_eq!(config.endpoint(), DEFAULT_ENDPOINT);
        assert_eq!(config.emulator_host(), None);
        assert_eq!(config.channel_pool_size(), DEFAULT_CHANNEL_POOL_SIZE);
        assert_eq!(config.connect_timeout(), DEFAULT_CONNECT_TIMEOUT);
        assert_eq!(config.request_timeout(), DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(config.keep_alive_interval(), DEFAULT_KEEP_ALIVE_INTERVAL);
        assert_eq!(config.keep_alive_timeout(), DEFAULT_KEEP_ALIVE_TIMEOUT);
        assert_eq!(config.service_endpoint(), DEFAULT_ENDPOINT);
        assert!(!config.uses_emulator());
    }

    #[test]
    fn new_trims_ids() {
        let config = ClientConfig::new(" project ", "\tinstance\n").expect("valid config");

        assert_eq!(config.project_id(), "project");
        assert_eq!(config.instance_id(), "instance");
    }

    #[test]
    fn builders_set_every_option() {
        let config = ClientConfig::new("project", "instance")
            .expect("valid config")
            .with_app_profile_id(" analytics ")
            .expect("valid app profile")
            .with_endpoint("https://example.test")
            .expect("valid endpoint")
            .with_emulator_host("127.0.0.1:8086")
            .expect("valid emulator")
            .with_channel_pool_size(3)
            .expect("valid pool")
            .with_connect_timeout(Duration::from_secs(2))
            .expect("valid timeout")
            .with_request_timeout(Duration::from_secs(3))
            .expect("valid timeout")
            .with_keep_alive(Duration::from_secs(4), Duration::from_secs(5))
            .expect("valid keepalive");

        assert_eq!(config.app_profile_id(), "analytics");
        assert_eq!(config.endpoint(), "https://example.test");
        assert_eq!(config.emulator_host(), Some("http://127.0.0.1:8086"));
        assert_eq!(config.service_endpoint(), "http://127.0.0.1:8086");
        assert_eq!(config.channel_pool_size(), 3);
        assert_eq!(config.connect_timeout(), Duration::from_secs(2));
        assert_eq!(config.request_timeout(), Duration::from_secs(3));
        assert_eq!(config.keep_alive_interval(), Duration::from_secs(4));
        assert_eq!(config.keep_alive_timeout(), Duration::from_secs(5));
        assert!(config.uses_emulator());
    }

    #[test]
    fn emulator_builder_keeps_explicit_scheme() {
        let config = ClientConfig::new("project", "instance")
            .expect("valid config")
            .with_emulator_host("https://localhost:8086")
            .expect("valid emulator");

        assert_eq!(config.emulator_host(), Some("https://localhost:8086"));
    }

    #[test]
    fn load_reads_all_environment_values() {
        Jail::expect_with(|jail| {
            jail.set_env("BIGTABLE_PROJECT_ID", "project");
            jail.set_env("BIGTABLE_INSTANCE_ID", "instance");
            jail.set_env("BIGTABLE_APP_PROFILE_ID", "analytics");
            jail.set_env("BIGTABLE_ENDPOINT", "https://example.test");
            jail.set_env("BIGTABLE_EMULATOR_HOST", "localhost:8086");
            jail.set_env("BIGTABLE_CHANNEL_POOL_SIZE", "4");
            jail.set_env("BIGTABLE_CONNECT_TIMEOUT", "1s");
            jail.set_env("BIGTABLE_REQUEST_TIMEOUT", "2s");
            jail.set_env("BIGTABLE_KEEP_ALIVE_INTERVAL", "3s");
            jail.set_env("BIGTABLE_KEEP_ALIVE_TIMEOUT", "4s");

            let config = ClientConfig::load().expect("valid environment");

            assert_eq!(config.project_id(), "project");
            assert_eq!(config.instance_id(), "instance");
            assert_eq!(config.app_profile_id(), "analytics");
            assert_eq!(config.endpoint(), "https://example.test");
            assert_eq!(config.emulator_host(), Some("http://localhost:8086"));
            assert_eq!(config.channel_pool_size(), 4);
            assert_eq!(config.connect_timeout(), Duration::from_secs(1));
            assert_eq!(config.request_timeout(), Duration::from_secs(2));
            assert_eq!(config.keep_alive_interval(), Duration::from_secs(3));
            assert_eq!(config.keep_alive_timeout(), Duration::from_secs(4));
            Ok(())
        });
    }

    #[test]
    fn load_reports_parse_errors() {
        Jail::expect_with(|jail| {
            jail.set_env("BIGTABLE_PROJECT_ID", "project");
            jail.set_env("BIGTABLE_INSTANCE_ID", "instance");
            jail.set_env("BIGTABLE_CHANNEL_POOL_SIZE", "many");

            let error = ClientConfig::load().expect_err("pool size must be numeric");

            assert!(matches!(error, Error::ConfigLoad { .. }));
            Ok(())
        });
    }

    #[test]
    fn every_empty_text_field_is_rejected() {
        let cases = [
            (ClientConfig::new("", "instance"), ConfigField::ProjectId),
            (ClientConfig::new("project", "  "), ConfigField::InstanceId),
            (
                ClientConfig::new("project", "instance")
                    .expect("valid config")
                    .with_app_profile_id("\t"),
                ConfigField::AppProfileId,
            ),
        ];

        for (result, field) in &cases {
            assert_invalid(result, *field, ConfigIssue::Empty);
        }
    }

    #[test]
    fn oversized_app_profile_id_is_rejected() {
        let result = ClientConfig::new("project", "instance")
            .expect("valid config")
            .with_app_profile_id("a".repeat(51));

        assert_invalid(
            &result,
            ConfigField::AppProfileId,
            ConfigIssue::TooLong { max_chars: 50 },
        );
    }

    #[test]
    fn every_invalid_uri_is_rejected() {
        let cases = [
            (
                ClientConfig::new("project", "instance")
                    .expect("valid config")
                    .with_endpoint("localhost:443"),
                ConfigField::Endpoint,
            ),
            (
                ClientConfig::new("project", "instance")
                    .expect("valid config")
                    .with_endpoint("ftp://example.test"),
                ConfigField::Endpoint,
            ),
            (
                ClientConfig::new("project", "instance")
                    .expect("valid config")
                    .with_emulator_host("ftp://localhost:8086"),
                ConfigField::EmulatorHost,
            ),
            (
                ClientConfig::new("project", "instance")
                    .expect("valid config")
                    .with_endpoint("https://example.test/v2"),
                ConfigField::Endpoint,
            ),
            (
                ClientConfig::new("project", "instance")
                    .expect("valid config")
                    .with_emulator_host("http://localhost:8086?debug=true"),
                ConfigField::EmulatorHost,
            ),
        ];

        for (result, field) in &cases {
            assert_invalid(result, *field, ConfigIssue::InvalidUri);
        }
    }

    #[test]
    fn every_zero_numeric_value_is_rejected() {
        let base = || ClientConfig::new("project", "instance").expect("valid config");
        let cases = [
            (
                base().with_channel_pool_size(0),
                ConfigField::ChannelPoolSize,
            ),
            (
                base().with_connect_timeout(Duration::ZERO),
                ConfigField::ConnectTimeout,
            ),
            (
                base().with_request_timeout(Duration::ZERO),
                ConfigField::RequestTimeout,
            ),
            (
                base().with_keep_alive(Duration::ZERO, Duration::from_secs(1)),
                ConfigField::KeepAliveInterval,
            ),
            (
                base().with_keep_alive(Duration::from_secs(1), Duration::ZERO),
                ConfigField::KeepAliveTimeout,
            ),
        ];

        for (result, field) in &cases {
            assert_invalid(result, *field, ConfigIssue::MustBePositive);
        }
    }

    fn assert_invalid(
        result: &Result<ClientConfig, Error>,
        field: ConfigField,
        issue: ConfigIssue,
    ) {
        assert!(matches!(
            result,
            Err(Error::InvalidConfig {
                field: actual_field,
                issue: actual_issue,
            }) if *actual_field == field && *actual_issue == issue
        ));
    }
}
