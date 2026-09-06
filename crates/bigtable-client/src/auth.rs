use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime},
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use gcp_auth::TokenProvider;
use tokio::task::JoinHandle;
use tonic::metadata::{Ascii, MetadataValue};

use crate::Error;

const BIGTABLE_DATA_SCOPE: &str = "https://www.googleapis.com/auth/bigtable.data";
const REFRESH_MARGIN: Duration = Duration::from_secs(15);
const REFRESH_RETRY_DELAY: Duration = Duration::from_secs(5);

pub(crate) struct TokenManager {
    current: Arc<ArcSwap<TokenSnapshot>>,
    refresh_task: Option<JoinHandle<()>>,
}

impl TokenManager {
    pub(crate) async fn start(source: Arc<dyn TokenSource>) -> Result<Arc<Self>, Error> {
        let token = source.token().await?;
        let current = Arc::new(ArcSwap::from_pointee(TokenSnapshot::try_from(token)?));
        let refresh_task = tokio::spawn(refresh_loop(Arc::clone(&source), Arc::clone(&current)));

        Ok(Arc::new(Self {
            current,
            refresh_task: Some(refresh_task),
        }))
    }

    pub(crate) fn authorization(&self) -> Result<MetadataValue<Ascii>, tonic::Status> {
        let snapshot = self.current.load();
        if SystemTime::now() >= snapshot.expires_at {
            return Err(tonic::Status::unauthenticated(
                "Bigtable access token expired before it could be refreshed",
            ));
        }
        Ok(snapshot.authorization.clone())
    }

    #[cfg(test)]
    pub(crate) fn from_static(secret: &str, expires_at: SystemTime) -> Result<Arc<Self>, Error> {
        let current = Arc::new(ArcSwap::from_pointee(TokenSnapshot::try_from(
            AccessToken {
                secret: secret.to_owned(),
                expires_at,
            },
        )?));
        Ok(Arc::new(Self {
            current,
            refresh_task: None,
        }))
    }
}

impl fmt::Debug for TokenManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenManager")
            .field("authorization", &"[redacted]")
            .finish_non_exhaustive()
    }
}

impl Drop for TokenManager {
    fn drop(&mut self) {
        if let Some(task) = self.refresh_task.take() {
            task.abort();
        }
    }
}

struct TokenSnapshot {
    authorization: MetadataValue<Ascii>,
    expires_at: SystemTime,
}

impl TryFrom<AccessToken> for TokenSnapshot {
    type Error = Error;

    fn try_from(token: AccessToken) -> Result<Self, Self::Error> {
        let mut authorization: MetadataValue<Ascii> = format!("Bearer {}", token.secret)
            .parse()
            .map_err(|source| Error::InvalidAccessToken { source })?;
        authorization.set_sensitive(true);
        Ok(Self {
            authorization,
            expires_at: token.expires_at,
        })
    }
}

impl fmt::Debug for TokenSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenSnapshot")
            .field("authorization", &"[redacted]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

pub(crate) struct AccessToken {
    secret: String,
    expires_at: SystemTime,
}

#[async_trait]
pub(crate) trait TokenSource: Send + Sync {
    async fn token(&self) -> Result<AccessToken, Error>;
}

pub(crate) struct GcpTokenSource {
    provider: Arc<dyn TokenProvider>,
}

impl GcpTokenSource {
    pub(crate) fn new(provider: Arc<dyn TokenProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl TokenSource for GcpTokenSource {
    async fn token(&self) -> Result<AccessToken, Error> {
        let token = self
            .provider
            .token(&[BIGTABLE_DATA_SCOPE])
            .await
            .map_err(|source| Error::Authentication {
                source: Box::new(source),
            })?;

        Ok(AccessToken {
            secret: token.as_str().to_owned(),
            expires_at: token.expires_at().into(),
        })
    }
}

async fn refresh_loop(source: Arc<dyn TokenSource>, current: Arc<ArcSwap<TokenSnapshot>>) {
    loop {
        let refresh_at = current
            .load()
            .expires_at
            .checked_sub(REFRESH_MARGIN)
            .unwrap_or(SystemTime::now());
        let delay = refresh_at
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
        tokio::time::sleep(delay).await;

        match source.token().await.and_then(TokenSnapshot::try_from) {
            Ok(token) => {
                let advanced_expiration = token.expires_at > current.load().expires_at;
                current.store(Arc::new(token));
                if !advanced_expiration {
                    // Cached providers can return the same token inside the refresh margin.
                    tokio::time::sleep(REFRESH_RETRY_DELAY).await;
                }
            }
            Err(error) => {
                tracing::warn!(error = %error, "failed to refresh Bigtable access token");
                tokio::time::sleep(REFRESH_RETRY_DELAY).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime},
    };

    use async_trait::async_trait;

    use super::{AccessToken, TokenManager, TokenSource};
    use crate::Error;

    struct FakeTokenSource {
        tokens: Mutex<VecDeque<Result<AccessToken, Error>>>,
    }

    #[async_trait]
    impl TokenSource for FakeTokenSource {
        async fn token(&self) -> Result<AccessToken, Error> {
            self.tokens
                .lock()
                .expect("fake token lock should not be poisoned")
                .pop_front()
                .unwrap_or(Err(Error::ChannelPoolClosed))
        }
    }

    #[tokio::test]
    async fn manager_returns_initial_bearer_token() {
        let manager = TokenManager::start(fake_source([token("first", 3_600)]))
            .await
            .expect("valid token");

        assert_eq!(
            manager.authorization().expect("current token"),
            "Bearer first"
        );
        let authorization = manager.authorization().expect("current token");
        assert!(authorization.is_sensitive());
        assert!(!format!("{authorization:?}").contains("first"));
    }

    #[tokio::test]
    async fn manager_refreshes_before_expiry() {
        let manager = TokenManager::start(fake_source([token("first", 1), token("second", 3_600)]))
            .await
            .expect("valid token");
        let expected = "Bearer second".parse().expect("valid metadata");

        for _ in 0..100 {
            if manager.authorization().ok().as_ref() == Some(&expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        panic!("token was not refreshed");
    }

    #[tokio::test(start_paused = true)]
    async fn unchanged_expiration_backs_off_and_stops_when_manager_drops() {
        struct CachedTokenSource {
            calls: AtomicUsize,
            expires_at: SystemTime,
        }

        #[async_trait]
        impl TokenSource for CachedTokenSource {
            async fn token(&self) -> Result<AccessToken, Error> {
                self.calls.fetch_add(1, Ordering::Relaxed);
                Ok(AccessToken {
                    secret: "cached".to_owned(),
                    expires_at: self.expires_at,
                })
            }
        }

        let source = Arc::new(CachedTokenSource {
            calls: AtomicUsize::new(0),
            expires_at: SystemTime::now() + Duration::from_secs(1),
        });
        let manager = TokenManager::start(source.clone())
            .await
            .expect("initial token");
        for _ in 0..20 {
            tokio::time::advance(Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
        }

        assert_eq!(source.calls.load(Ordering::Relaxed), 2);
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(source.calls.load(Ordering::Relaxed), 3);

        drop(manager);
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(source.calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn manager_recovers_after_refresh_failure() {
        let manager = TokenManager::start(fake_results([
            Ok(token("first", 1)),
            Err(Error::ChannelPoolClosed),
            Ok(token("recovered", 3_600)),
        ]))
        .await
        .expect("initial token is valid");
        let expected = "Bearer recovered".parse().expect("valid metadata");

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        for _ in 0..20 {
            if manager.authorization().ok().as_ref() == Some(&expected) {
                return;
            }
            tokio::task::yield_now().await;
        }

        panic!("token refresh did not recover");
    }

    #[tokio::test]
    async fn manager_reports_initial_source_failure() {
        let error = TokenManager::start(fake_results([Err(Error::ChannelPoolClosed)]))
            .await
            .expect_err("initial token is required");

        assert!(matches!(error, Error::ChannelPoolClosed));
    }

    #[tokio::test]
    async fn manager_rejects_invalid_token_metadata() {
        let error = TokenManager::start(fake_source([token("bad\nvalue", 3_600)]))
            .await
            .expect_err("invalid metadata must be rejected");

        assert!(matches!(error, Error::InvalidAccessToken { .. }));
    }

    #[tokio::test]
    async fn manager_rejects_expired_token() {
        let manager = TokenManager::start(fake_source([token("expired", 0)]))
            .await
            .expect("metadata is valid");

        let status = manager
            .authorization()
            .expect_err("expired tokens must not be sent");

        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn manager_debug_redacts_token() {
        let manager = TokenManager::start(fake_source([token("very-secret", 3_600)]))
            .await
            .expect("valid token");
        let debug = format!("{manager:?}");

        assert!(debug.contains("[redacted]"));
        assert!(!debug.contains("very-secret"));
    }

    fn fake_source<const N: usize>(tokens: [AccessToken; N]) -> Arc<dyn TokenSource> {
        fake_results(tokens.map(Ok))
    }

    fn fake_results<const N: usize>(
        tokens: [Result<AccessToken, Error>; N],
    ) -> Arc<dyn TokenSource> {
        Arc::new(FakeTokenSource {
            tokens: Mutex::new(tokens.into()),
        })
    }

    fn token(secret: &str, lifetime_seconds: u64) -> AccessToken {
        AccessToken {
            secret: secret.to_owned(),
            expires_at: SystemTime::now() + Duration::from_secs(lifetime_seconds),
        }
    }
}
