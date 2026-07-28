use std::{fmt, sync::Arc};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use gcp_auth::TokenProvider;
use prost::Message;
use tonic::{
    Request, Status,
    metadata::{Ascii, MetadataValue},
    service::{Interceptor, interceptor::InterceptedService},
    transport::Channel,
};

use crate::{
    BulkMutation, BulkMutationOptions, BulkMutationResult, ClientConfig, Error, Query, ReadOptions,
    Row, RowMutation, RowStream,
    auth::{GcpTokenSource, TokenManager},
    channel,
    proto::{FeatureFlags, bigtable_client::BigtableClient},
    read, write,
};

const API_CLIENT_HEADER: &str = concat!(
    "gl-rust/",
    env!("CARGO_PKG_RUST_VERSION"),
    " gccl/",
    env!("CARGO_PKG_VERSION")
);

/// The generated Tonic Bigtable client with standard client metadata.
pub type RawClient = BigtableClient<InterceptedService<Channel, AuthInterceptor>>;

/// A cloneable Bigtable data client.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    /// Connects with Google application default credentials.
    ///
    /// Emulator connections skip authentication.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Authentication`] when credentials cannot be found or a
    /// token cannot be fetched. Returns [`Error::Transport`] when the gRPC
    /// channel cannot connect.
    pub async fn connect(config: ClientConfig) -> Result<Self, Error> {
        if config.uses_emulator() {
            return Self::connect_inner(config, None).await;
        }

        let provider = gcp_auth::provider()
            .await
            .map_err(|source| Error::Authentication {
                source: Box::new(source),
            })?;
        Self::connect_with_token_provider(config, provider).await
    }

    /// Connects with a caller-provided Google Cloud token provider.
    ///
    /// The provider is ignored for emulator connections.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Authentication`] when a token cannot be fetched.
    /// Returns [`Error::Transport`] when the gRPC channel cannot connect.
    pub async fn connect_with_token_provider(
        config: ClientConfig,
        provider: Arc<dyn TokenProvider>,
    ) -> Result<Self, Error> {
        if config.uses_emulator() {
            return Self::connect_inner(config, None).await;
        }

        let tokens = TokenManager::start(Arc::new(GcpTokenSource::new(provider))).await?;
        Self::connect_inner(config, Some(tokens)).await
    }

    /// Returns the configuration used by this client.
    #[must_use]
    pub fn config(&self) -> &ClientConfig {
        &self.inner.config
    }

    /// Returns a clone of the generated Tonic client.
    ///
    /// Raw requests receive authentication and standard feature metadata.
    /// Callers must add the `x-goog-request-params` routing header.
    #[must_use]
    pub fn raw_client(&self) -> RawClient {
        self.inner.raw.clone()
    }

    /// Starts a high-level streaming row query.
    ///
    /// The client assembles complete rows, retries transient failures, and
    /// resumes after the latest safe row key.
    ///
    /// # Errors
    ///
    /// Returns an error when the retry policy is invalid, routing metadata
    /// cannot be encoded, or the first RPC cannot be started. Errors after the
    /// stream starts are returned as stream items.
    pub async fn read_rows(&self, query: Query) -> Result<RowStream, Error> {
        let mut options = ReadOptions::default();
        options.deadlines.operation_timeout = self.config().request_timeout();
        read::start(self.raw_client(), self.config(), query, options).await
    }

    /// Starts a row query with caller-provided retry and deadline settings.
    ///
    /// # Errors
    ///
    /// Returns an error when the policy is invalid, routing metadata cannot be
    /// encoded, or the first RPC cannot be started. Errors after the stream
    /// starts are returned as stream items.
    pub async fn read_rows_with_options(
        &self,
        query: Query,
        options: ReadOptions,
    ) -> Result<RowStream, Error> {
        read::start(self.raw_client(), self.config(), query, options).await
    }

    /// Reads one row by exact key.
    ///
    /// # Errors
    ///
    /// Returns query, policy, gRPC, deadline, or row assembly errors.
    pub async fn read_row(
        &self,
        table_id: impl Into<String>,
        row_key: impl Into<bytes::Bytes>,
    ) -> Result<Option<Row>, Error> {
        let query = Query::new(table_id)?.row_key(row_key).limit(1)?;
        let mut rows = self.read_rows(query).await?;
        match rows.next().await {
            Some(row) => row.map(Some),
            None => Ok(None),
        }
    }

    /// Applies row mutations in bounded concurrent batches.
    ///
    /// Each row entry is atomic. Entries may run in any order. The client
    /// retries only unresolved entries that are safe to replay.
    ///
    /// # Errors
    ///
    /// Returns policy or metadata errors before sending. Returns
    /// [`Error::BulkMutation`] with original entry indexes when one or more
    /// entries lack a confirmed success.
    pub async fn mutate_rows(&self, mutation: BulkMutation) -> Result<BulkMutationResult, Error> {
        let mut options = BulkMutationOptions::default();
        options.deadlines.operation_timeout = self.config().request_timeout();
        write::execute(self.raw_client(), self.config(), mutation, options).await
    }

    /// Applies row mutations with caller-provided policies.
    ///
    /// # Errors
    ///
    /// Returns policy or metadata errors before sending. Returns
    /// [`Error::BulkMutation`] with original entry indexes when one or more
    /// entries lack a confirmed success.
    pub async fn mutate_rows_with_options(
        &self,
        mutation: BulkMutation,
        options: BulkMutationOptions,
    ) -> Result<BulkMutationResult, Error> {
        write::execute(self.raw_client(), self.config(), mutation, options).await
    }

    /// Applies one atomic row mutation.
    ///
    /// # Errors
    ///
    /// Returns mutation, policy, metadata, deadline, or gRPC errors.
    pub async fn mutate_row(
        &self,
        table_id: impl Into<String>,
        mutation: RowMutation,
    ) -> Result<(), Error> {
        let bulk = BulkMutation::new(table_id)?.entry(mutation)?;
        self.mutate_rows(bulk).await?;
        Ok(())
    }

    /// Applies one atomic row mutation with caller-provided policies.
    ///
    /// # Errors
    ///
    /// Returns mutation, policy, metadata, deadline, or gRPC errors.
    pub async fn mutate_row_with_options(
        &self,
        table_id: impl Into<String>,
        mutation: RowMutation,
        options: BulkMutationOptions,
    ) -> Result<(), Error> {
        let bulk = BulkMutation::new(table_id)?.entry(mutation)?;
        self.mutate_rows_with_options(bulk, options).await?;
        Ok(())
    }

    async fn connect_inner(
        config: ClientConfig,
        tokens: Option<Arc<TokenManager>>,
    ) -> Result<Self, Error> {
        let channel = channel::connect(&config).await?;
        let interceptor = AuthInterceptor::new(tokens)?;
        let raw = BigtableClient::with_interceptor(channel, interceptor);

        Ok(Self {
            inner: Arc::new(ClientInner { config, raw }),
        })
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Client")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

struct ClientInner {
    config: ClientConfig,
    raw: RawClient,
}

/// Adds authentication and standard Bigtable metadata to raw Tonic requests.
#[derive(Clone)]
pub struct AuthInterceptor {
    tokens: Option<Arc<TokenManager>>,
    api_client: MetadataValue<Ascii>,
    feature_flags: MetadataValue<Ascii>,
}

impl AuthInterceptor {
    fn new(tokens: Option<Arc<TokenManager>>) -> Result<Self, Error> {
        let api_client = MetadataValue::from_static(API_CLIENT_HEADER);
        let feature_flags = feature_flags_header()?;
        Ok(Self {
            tokens,
            api_client,
            feature_flags,
        })
    }
}

impl fmt::Debug for AuthInterceptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthInterceptor")
            .field("authenticated", &self.tokens.is_some())
            .field("api_client", &self.api_client)
            .field("feature_flags", &self.feature_flags)
            .finish()
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(tokens) = &self.tokens {
            request
                .metadata_mut()
                .insert("authorization", tokens.authorization()?);
        }
        request
            .metadata_mut()
            .insert("x-goog-api-client", self.api_client.clone());
        request
            .metadata_mut()
            .insert("bigtable-features", self.feature_flags.clone());
        Ok(request)
    }
}

fn feature_flags_header() -> Result<MetadataValue<Ascii>, Error> {
    let flags = FeatureFlags {
        reverse_scans: true,
        last_scanned_row_responses: true,
        retry_info: true,
        ..FeatureFlags::default()
    };
    URL_SAFE_NO_PAD
        .encode(flags.encode_to_vec())
        .parse()
        .map_err(|source| Error::InvalidClientMetadata {
            header: "bigtable-features",
            source,
        })
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use prost::Message;
    use tonic::{Request, service::Interceptor};

    use super::{API_CLIENT_HEADER, AuthInterceptor};
    use crate::{auth::TokenManager, proto::FeatureFlags};

    #[test]
    fn anonymous_interceptor_adds_standard_headers() {
        let mut interceptor = AuthInterceptor::new(None).expect("valid static metadata");
        let request = interceptor
            .call(Request::new(()))
            .expect("metadata should be valid");

        assert_eq!(
            request.metadata().get("x-goog-api-client"),
            Some(&API_CLIENT_HEADER.parse().expect("valid metadata"))
        );
        assert!(request.metadata().contains_key("bigtable-features"));
        assert!(!request.metadata().contains_key("authorization"));
    }

    #[test]
    fn feature_header_matches_supported_behavior() {
        let mut interceptor = AuthInterceptor::new(None).expect("valid static metadata");
        let request = interceptor
            .call(Request::new(()))
            .expect("metadata should be valid");
        let encoded = request
            .metadata()
            .get("bigtable-features")
            .expect("feature header")
            .to_str()
            .expect("ASCII metadata");
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .expect("base64 feature flags");
        let flags = FeatureFlags::decode(bytes.as_slice()).expect("feature flags protobuf");

        assert!(flags.reverse_scans);
        assert!(flags.last_scanned_row_responses);
        assert!(!flags.mutate_rows_rate_limit);
        assert!(!flags.mutate_rows_rate_limit2);
        assert!(!flags.routing_cookie);
        assert!(flags.retry_info);
        assert!(!flags.client_side_metrics_enabled);
        assert!(!flags.traffic_director_enabled);
        assert!(!flags.direct_access_requested);
        assert!(!flags.peer_info);
    }

    #[test]
    fn authenticated_interceptor_adds_bearer_token() {
        let tokens =
            TokenManager::from_static("secret", SystemTime::now() + Duration::from_secs(60))
                .expect("valid token");
        let mut interceptor = AuthInterceptor::new(Some(tokens)).expect("valid static metadata");
        let request = interceptor.call(Request::new(())).expect("current token");

        assert_eq!(
            request.metadata().get("authorization"),
            Some(&"Bearer secret".parse().expect("valid metadata"))
        );
    }

    #[test]
    fn authenticated_interceptor_rejects_expired_token() {
        let tokens = TokenManager::from_static("expired", SystemTime::UNIX_EPOCH)
            .expect("valid token metadata");
        let mut interceptor = AuthInterceptor::new(Some(tokens)).expect("valid static metadata");
        let status = interceptor
            .call(Request::new(()))
            .expect_err("expired tokens cannot be sent");

        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn interceptor_debug_contains_no_credentials() {
        let interceptor = AuthInterceptor::new(None).expect("valid static metadata");
        let debug = format!("{interceptor:?}");

        assert!(debug.contains("authenticated: false"));
        assert!(!debug.contains("Bearer"));
    }
}
