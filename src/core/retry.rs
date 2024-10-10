use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
/// Retry strategy for jobs
pub enum RetryStrategy {
    /// Linear backoff in seconds
    ///
    /// For example, `RetryStrategy::Linear(5)` will retry a failed job 5 seconds after the previous attempt
    Linear { interval: usize },

    /// Exponential backoff with a base interval in seconds
    ///
    /// For example, `RetryStrategy::Exponential(2)` will retry a failed job 2 seconds after the first failure,
    /// 4 seconds after the second failure, and so on
    Exponential { base: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
/// Retry policy to define how and when jobs should be retried
pub struct RetryPolicy {
    /// Strategy for retrying failed jobs
    pub strategy: RetryStrategy,

    /// Maximum number of retries or retry indefinitely
    pub max_retries: Option<usize>, // `None` means infinite retries
}

impl RetryPolicy {
    /// Create a new linear retry policy with a maximum number of retries
    pub const fn linear(interval: usize, max_retries: Option<usize>) -> Self {
        RetryPolicy {
            strategy: RetryStrategy::Linear { interval },
            max_retries,
        }
    }

    /// Create a new exponential retry policy with a maximum number of retries
    pub const fn exponential(base: usize, max_retries: Option<usize>) -> Self {
        RetryPolicy {
            strategy: RetryStrategy::Exponential { base },
            max_retries,
        }
    }

    pub fn wait_time(&self, retry_count: usize) -> usize {
        match self.strategy {
            RetryStrategy::Linear { interval } => interval,
            RetryStrategy::Exponential { base } => {
                base.saturating_pow(retry_count as u32)
            }
        }
    }
}
