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
    let mut delay = refresh_delay(current.load().expires_at, SystemTime::now());
    loop {
        tokio::time::sleep(delay).await;

        match source.token().await.and_then(TokenSnapshot::try_from) {
            Ok(token) => {
                let advanced_expiration = token.expires_at > current.load().expires_at;
                let now = SystemTime::now();
                delay = refresh_delay(token.expires_at, now);
                if !advanced_expiration || delay.is_zero() {
                    delay = refresh_retry_delay(token.expires_at, now);
                }
                current.store(Arc::new(token));
            }
            Err(error) => {
                tracing::warn!(error = %error, "failed to refresh Bigtable access token");
                delay = REFRESH_RETRY_DELAY;
            }
        }
    }
}

fn refresh_delay(expires_at: SystemTime, now: SystemTime) -> Duration {
    let remaining = expires_at.duration_since(now).unwrap_or(Duration::ZERO);
    // Keep the standard margin for long-lived tokens and refresh short-lived tokens halfway.
    remaining - REFRESH_MARGIN.min(remaining / 2)
}

fn refresh_retry_delay(expires_at: SystemTime, now: SystemTime) -> Duration {
    match expires_at.duration_since(now) {
        // A caching provider may renew only at expiry; wait directly to that boundary.
        Ok(remaining) if !remaining.is_zero() => REFRESH_RETRY_DELAY.min(remaining),
        _ => REFRESH_RETRY_DELAY,
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

    use super::{AccessToken, TokenManager, TokenSource, refresh_retry_delay};
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

    #[test]
    fn cached_retry_delay_caps_at_expiry_and_is_never_zero() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        for (expires_at, expected) in [
            (now + Duration::from_secs(30), Duration::from_secs(5)),
            (now + Duration::from_millis(500), Duration::from_millis(500)),
            (now + Duration::from_nanos(1), Duration::from_nanos(1)),
            (now, Duration::from_secs(5)),
            (now - Duration::from_nanos(1), Duration::from_secs(5)),
        ] {
            assert_eq!(refresh_retry_delay(expires_at, now), expected);
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

    #[tokio::test(start_paused = true)]
    async fn manager_refreshes_long_lived_tokens_at_standard_margin() {
        let manager =
            TokenManager::start(fake_source([token("first", 3_600), token("second", 7_200)]))
                .await
                .expect("valid token");

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(3_584)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            manager.authorization().expect("current token"),
            "Bearer first"
        );
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            manager.authorization().expect("refreshed token"),
            "Bearer second"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn advancing_short_lived_tokens_wait_between_refreshes() {
        struct ShortLivedTokenSource(AtomicUsize);

        #[async_trait]
        impl TokenSource for ShortLivedTokenSource {
            async fn token(&self) -> Result<AccessToken, Error> {
                self.0.fetch_add(1, Ordering::Relaxed);
                Ok(token("short-lived", 1))
            }
        }

        let source = Arc::new(ShortLivedTokenSource(AtomicUsize::new(0)));
        let _manager = TokenManager::start(source.clone())
            .await
            .expect("initial token");
        for _ in 0..20 {
            tokio::time::advance(Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(source.0.load(Ordering::Relaxed), 1);

        // Pausing Tokio does not advance SystemTime: every fetched token still
        // has about one real second remaining, so each refresh waits about half a second.
        for expected_calls in 2..=3 {
            tokio::time::advance(Duration::from_millis(600)).await;
            tokio::task::yield_now().await;
            assert_eq!(source.0.load(Ordering::Relaxed), expected_calls);
            tokio::time::advance(Duration::from_millis(20)).await;
            tokio::task::yield_now().await;
            assert_eq!(source.0.load(Ordering::Relaxed), expected_calls);
        }
    }

    #[tokio::test]
    async fn cached_short_lived_token_refreshes_when_provider_renews_at_expiry() {
        struct RenewAtExpiry {
            expires_at: SystemTime,
            cached_calls: AtomicUsize,
            refreshed: tokio::sync::Notify,
        }

        #[async_trait]
        impl TokenSource for RenewAtExpiry {
            async fn token(&self) -> Result<AccessToken, Error> {
                if SystemTime::now() < self.expires_at {
                    self.cached_calls.fetch_add(1, Ordering::Relaxed);
                    Ok(AccessToken {
                        secret: "cached".to_owned(),
                        expires_at: self.expires_at,
                    })
                } else {
                    self.refreshed.notify_one();
                    Ok(token("renewed", 3_600))
                }
            }
        }

        let source = Arc::new(RenewAtExpiry {
            expires_at: SystemTime::now() + Duration::from_secs(1),
            cached_calls: AtomicUsize::new(0),
            refreshed: tokio::sync::Notify::new(),
        });
        let manager = TokenManager::start(source.clone())
            .await
            .expect("initial token");
        tokio::time::timeout(Duration::from_secs(2), source.refreshed.notified())
            .await
            .expect("refresh follows expiry without a five-second gap");

        assert!(source.cached_calls.load(Ordering::Relaxed) >= 2);
        assert_eq!(
            manager.authorization().expect("current token"),
            "Bearer renewed"
        );
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

        assert_eq!(source.calls.load(Ordering::Relaxed), 1);
        tokio::time::advance(Duration::from_millis(600)).await;
        tokio::task::yield_now().await;
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

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(600)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            manager.authorization().expect("cached token"),
            "Bearer first"
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            manager.authorization().expect("refreshed token"),
            "Bearer recovered"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn advancing_but_expired_tokens_wait_before_retrying() {
        let expired = |seconds| AccessToken {
            secret: "expired".to_owned(),
            expires_at: SystemTime::UNIX_EPOCH + Duration::from_secs(seconds),
        };
        let manager = TokenManager::start(fake_source([
            expired(0),
            expired(1),
            token("recovered", 3_600),
        ]))
        .await
        .expect("valid metadata");
        for _ in 0..20 {
            tokio::time::advance(Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
        }
        assert!(manager.authorization().is_err());

        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            manager.authorization().expect("refreshed token"),
            "Bearer recovered"
        );
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
