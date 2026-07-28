use std::time::Duration;

use googleapis_tonic_google_rpc::google::rpc::{RetryInfo, Status as RpcStatus};
use prost::Message;
use tonic::{Code, Status};

use crate::{Error, ReadPolicyIssue};

const RETRY_INFO_TYPE: &str = "type.googleapis.com/google.rpc.RetryInfo";

/// How retry backoff is randomized.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum Jitter {
    /// Pick a delay between zero and the calculated backoff.
    #[default]
    Full,
    /// Use the calculated backoff without randomization.
    None,
}

/// Retry settings for a high-level Bigtable operation.
#[derive(Clone, Debug, PartialEq)]
pub struct RetryPolicy {
    /// Total attempts, including the first request.
    pub max_attempts: u32,
    /// Backoff before the second attempt.
    pub initial_backoff: Duration,
    /// Largest calculated backoff.
    pub max_backoff: Duration,
    /// Backoff growth factor.
    pub multiplier: f64,
    /// Backoff randomization.
    pub jitter: Jitter,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(60),
            multiplier: 2.0,
            jitter: Jitter::Full,
        }
    }
}

/// Deadline settings for a high-level Bigtable operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadlinePolicy {
    /// Maximum time for all attempts and backoff.
    pub operation_timeout: Duration,
    /// Maximum time for one streaming RPC attempt.
    pub attempt_timeout: Duration,
}

impl Default for DeadlinePolicy {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(600),
            attempt_timeout: Duration::from_secs(20),
        }
    }
}

/// Per-operation settings for `ReadRows`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReadOptions {
    /// Retry behavior.
    pub retry: RetryPolicy,
    /// Attempt and operation deadlines.
    pub deadlines: DeadlinePolicy,
}

pub(crate) fn validate(options: &ReadOptions) -> Result<(), Error> {
    validate_policies(&options.retry, &options.deadlines)
        .map_err(|issue| Error::invalid_read_policy(issue.into()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PolicyIssue {
    ZeroMaxAttempts,
    ZeroInitialBackoff,
    ZeroMaxBackoff,
    MaxBackoffTooSmall,
    InvalidBackoffMultiplier,
    ZeroOperationTimeout,
    ZeroAttemptTimeout,
}

pub(crate) fn validate_policies(
    retry: &RetryPolicy,
    deadlines: &DeadlinePolicy,
) -> Result<(), PolicyIssue> {
    if retry.max_attempts == 0 {
        return Err(PolicyIssue::ZeroMaxAttempts);
    }
    if retry.initial_backoff.is_zero() {
        return Err(PolicyIssue::ZeroInitialBackoff);
    }
    if retry.max_backoff.is_zero() {
        return Err(PolicyIssue::ZeroMaxBackoff);
    }
    if retry.max_backoff < retry.initial_backoff {
        return Err(PolicyIssue::MaxBackoffTooSmall);
    }
    if !retry.multiplier.is_finite() || retry.multiplier < 1.0 {
        return Err(PolicyIssue::InvalidBackoffMultiplier);
    }
    if deadlines.operation_timeout.is_zero() {
        return Err(PolicyIssue::ZeroOperationTimeout);
    }
    if deadlines.attempt_timeout.is_zero() {
        return Err(PolicyIssue::ZeroAttemptTimeout);
    }
    Ok(())
}

impl From<PolicyIssue> for ReadPolicyIssue {
    fn from(issue: PolicyIssue) -> Self {
        match issue {
            PolicyIssue::ZeroMaxAttempts => Self::ZeroMaxAttempts,
            PolicyIssue::ZeroInitialBackoff => Self::ZeroInitialBackoff,
            PolicyIssue::ZeroMaxBackoff => Self::ZeroMaxBackoff,
            PolicyIssue::MaxBackoffTooSmall => Self::MaxBackoffTooSmall,
            PolicyIssue::InvalidBackoffMultiplier => Self::InvalidBackoffMultiplier,
            PolicyIssue::ZeroOperationTimeout => Self::ZeroOperationTimeout,
            PolicyIssue::ZeroAttemptTimeout => Self::ZeroAttemptTimeout,
        }
    }
}

pub(crate) fn is_retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Cancelled | Code::DeadlineExceeded | Code::Unavailable | Code::Aborted
    )
}

pub(crate) fn is_mutate_retryable(status: &Status) -> bool {
    matches!(status.code(), Code::DeadlineExceeded | Code::Unavailable)
}

pub(crate) fn backoff(policy: &RetryPolicy, failed_attempt: u32) -> Duration {
    let exponent = i32::try_from(failed_attempt.saturating_sub(1)).unwrap_or(i32::MAX);
    let factor = policy.multiplier.powi(exponent);
    let capped_seconds =
        (policy.initial_backoff.as_secs_f64() * factor).min(policy.max_backoff.as_secs_f64());
    let capped = Duration::from_secs_f64(capped_seconds);

    match policy.jitter {
        Jitter::None => capped,
        Jitter::Full => {
            let max_nanos = u64::try_from(capped.as_nanos()).unwrap_or(u64::MAX);
            Duration::from_nanos(fastrand::u64(0..=max_nanos))
        }
    }
}

pub(crate) fn retry_delay(status: &Status) -> Option<Duration> {
    let rich_status = RpcStatus::decode(status.details()).ok()?;
    let retry_info = rich_status
        .details
        .iter()
        .find(|detail| detail.type_url == RETRY_INFO_TYPE)
        .and_then(|detail| RetryInfo::decode(detail.value.as_slice()).ok())?;
    let delay = retry_info.retry_delay?;
    if delay.seconds < 0 || !(0..1_000_000_000).contains(&delay.nanos) {
        return None;
    }

    Some(Duration::new(
        u64::try_from(delay.seconds).ok()?,
        u32::try_from(delay.nanos).ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use googleapis_tonic_google_rpc::google::rpc::{RetryInfo, Status as RpcStatus};
    use prost::Message;
    use prost_types::Any;
    use tonic::{Code, Status};

    use super::{
        DeadlinePolicy, Jitter, RETRY_INFO_TYPE, ReadOptions, RetryPolicy, backoff,
        is_mutate_retryable, is_retryable, retry_delay, validate,
    };
    use crate::{Error, ReadPolicyIssue};

    #[test]
    fn defaults_match_bigtable_read_guidance() {
        let options = ReadOptions::default();

        assert_eq!(options.retry.max_attempts, 10);
        assert_eq!(
            options.retry.initial_backoff,
            std::time::Duration::from_millis(10)
        );
        assert_eq!(
            options.retry.max_backoff,
            std::time::Duration::from_secs(60)
        );
        assert_eq!(
            options.deadlines.operation_timeout,
            std::time::Duration::from_secs(600)
        );
        assert_eq!(
            options.deadlines.attempt_timeout,
            std::time::Duration::from_secs(20)
        );
    }

    #[test]
    fn deterministic_backoff_grows_and_caps() {
        let policy = RetryPolicy {
            jitter: Jitter::None,
            ..RetryPolicy::default()
        };

        assert_eq!(backoff(&policy, 1), std::time::Duration::from_millis(10));
        assert_eq!(backoff(&policy, 2), std::time::Duration::from_millis(20));
        assert_eq!(backoff(&policy, 20), std::time::Duration::from_secs(60));
        assert_eq!(
            backoff(&policy, u32::MAX),
            std::time::Duration::from_secs(60)
        );
    }

    #[test]
    fn full_jitter_stays_inside_the_calculated_backoff() {
        let policy = RetryPolicy::default();

        for _ in 0..100 {
            assert!(backoff(&policy, 1) <= policy.initial_backoff);
        }
    }

    #[test]
    fn read_retry_codes_match_google_clients() {
        for code in [
            Code::Cancelled,
            Code::DeadlineExceeded,
            Code::Unavailable,
            Code::Aborted,
        ] {
            assert!(is_retryable(&Status::new(code, "retry")));
        }
        for code in [
            Code::Internal,
            Code::ResourceExhausted,
            Code::InvalidArgument,
            Code::NotFound,
            Code::PermissionDenied,
            Code::Unauthenticated,
        ] {
            assert!(!is_retryable(&Status::new(code, "stop")));
        }
    }

    #[test]
    fn mutation_retry_codes_match_google_clients() {
        for code in [Code::DeadlineExceeded, Code::Unavailable] {
            assert!(is_mutate_retryable(&Status::new(code, "retry")));
        }
        for code in [
            Code::Cancelled,
            Code::Aborted,
            Code::Internal,
            Code::ResourceExhausted,
            Code::InvalidArgument,
            Code::NotFound,
            Code::PermissionDenied,
            Code::Unauthenticated,
        ] {
            assert!(!is_mutate_retryable(&Status::new(code, "stop")));
        }
    }

    #[test]
    fn retry_info_delay_is_decoded_from_rich_status() {
        let retry_info = RetryInfo {
            retry_delay: Some(prost_types::Duration {
                seconds: 2,
                nanos: 5,
            }),
        };
        let rich_status = RpcStatus {
            code: i32::from(Code::Unavailable),
            message: "retry".to_owned(),
            details: vec![Any {
                type_url: RETRY_INFO_TYPE.to_owned(),
                value: retry_info.encode_to_vec(),
            }],
        };
        let status = Status::with_details(
            Code::Unavailable,
            "retry",
            Bytes::from(rich_status.encode_to_vec()),
        );

        assert_eq!(retry_delay(&status), Some(std::time::Duration::new(2, 5)));
    }

    #[test]
    fn malformed_retry_info_is_ignored() {
        assert_eq!(
            retry_delay(&Status::with_details(
                Code::Unavailable,
                "retry",
                Bytes::from_static(b"not protobuf")
            )),
            None
        );
    }

    #[test]
    fn invalid_policy_values_return_typed_errors() {
        let cases = [
            (
                ReadOptions {
                    retry: RetryPolicy {
                        max_attempts: 0,
                        ..RetryPolicy::default()
                    },
                    ..ReadOptions::default()
                },
                ReadPolicyIssue::ZeroMaxAttempts,
            ),
            (
                ReadOptions {
                    retry: RetryPolicy {
                        initial_backoff: std::time::Duration::ZERO,
                        ..RetryPolicy::default()
                    },
                    ..ReadOptions::default()
                },
                ReadPolicyIssue::ZeroInitialBackoff,
            ),
            (
                ReadOptions {
                    retry: RetryPolicy {
                        max_backoff: std::time::Duration::ZERO,
                        ..RetryPolicy::default()
                    },
                    ..ReadOptions::default()
                },
                ReadPolicyIssue::ZeroMaxBackoff,
            ),
            (
                ReadOptions {
                    retry: RetryPolicy {
                        initial_backoff: std::time::Duration::from_secs(2),
                        max_backoff: std::time::Duration::from_secs(1),
                        ..RetryPolicy::default()
                    },
                    ..ReadOptions::default()
                },
                ReadPolicyIssue::MaxBackoffTooSmall,
            ),
            (
                ReadOptions {
                    retry: RetryPolicy {
                        multiplier: 0.5,
                        ..RetryPolicy::default()
                    },
                    ..ReadOptions::default()
                },
                ReadPolicyIssue::InvalidBackoffMultiplier,
            ),
            (
                ReadOptions {
                    deadlines: DeadlinePolicy {
                        operation_timeout: std::time::Duration::ZERO,
                        ..DeadlinePolicy::default()
                    },
                    ..ReadOptions::default()
                },
                ReadPolicyIssue::ZeroOperationTimeout,
            ),
            (
                ReadOptions {
                    deadlines: DeadlinePolicy {
                        attempt_timeout: std::time::Duration::ZERO,
                        ..DeadlinePolicy::default()
                    },
                    ..ReadOptions::default()
                },
                ReadPolicyIssue::ZeroAttemptTimeout,
            ),
        ];

        for (options, expected) in cases {
            assert!(matches!(
                validate(&options),
                Err(Error::InvalidReadPolicy { issue }) if issue == expected
            ));
        }
    }
}
