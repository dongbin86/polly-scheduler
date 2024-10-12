use crate::{
    core::{
        retry::{RetryPolicy, RetryStrategy},
        task_kind::TaskKind,
    },
    generate_token, utc_now,
};
use serde::{Deserialize, Serialize};
use std::fmt;
type LinearInterval = usize;
type ExponentialBase = usize;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskMeta {
    pub id: String,                     // Unique identifier for the task
    pub task_key: String,               // Key to identify the specific task
    pub task_params: String,            // Arguments for the task, stored as a JSON object
    pub queue_name: String,             // Name of the queue for the task
    pub updated_at: i64,                // Timestamp of the last update
    pub status: TaskStatus,             // Current status of the task
    pub stopped_reason: Option<String>, // Optional reason for why the task was stopped
    pub last_error: Option<String>,     // Error message from the last execution, if any
    pub last_run: i64,                  // Timestamp of the last run
    pub next_run: i64,                  // Timestamp of the next scheduled run
    pub kind: TaskKind,                 // Type of the task
    pub success_count: u32,             // Count of successful runs
    pub failure_count: u32,             // Count of failed runs
    pub runner_id: Option<String>,      // The ID of the current task runner, may be None
    pub retry_strategy: Retry,          // Retry strategy for handling failures
    pub retry_interval: usize,          // Interval for retrying the task
    pub base_interval: usize,           // Base interval for exponential backoff
    pub delay_seconds: u32,             //Delay before executing a Once task, specified in seconds
    pub max_retries: Option<usize>,     // Maximum number of retries allowed
    pub cron_schedule: Option<String>,  // Cron expression for scheduling
    pub cron_timezone: Option<String>,  // Timezone for the cron schedule (stored as a string)
    pub is_repeating: bool,             // Indicates if the task is repeating
    pub repeat_interval: u32,           // Interval for repeating task
    pub heartbeat_at: i64,              // Timestamp of the last heartbeat in milliseconds
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum TaskStatus {
    // Task has been scheduled but has not started executing yet.
    Scheduled,

    // Task is currently running.
    Running,

    // Task has completed successfully.
    Success,

    // Task has failed.
    Failed,

    // Task has been marked for removal and will be cleaned up by a dedicated thread.
    Removed,

    // Task has been stopped (applicable to Repeat and Cron type tasks).
    Stopped,
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status_str = match self {
            TaskStatus::Scheduled => "Scheduled",
            TaskStatus::Running => "Running",
            TaskStatus::Success => "Success",
            TaskStatus::Failed => "Failed",
            TaskStatus::Removed => "Removed",
            TaskStatus::Stopped => "Stopped",
        };
        write!(f, "{}", status_str)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Retry {
    Linear,
    Exponential,
}

fn to_retry(retry_policy: RetryPolicy) -> (Retry, LinearInterval, ExponentialBase) {
    match retry_policy.strategy {
        RetryStrategy::Linear { interval } => (Retry::Linear, interval, Default::default()),
        RetryStrategy::Exponential { base } => (Retry::Exponential, Default::default(), base),
    }
}

impl TaskMeta {
    pub fn new(
        task_key: String,
        task_params: String,
        queue_name: String,
        kind: TaskKind,
        retry_policy: RetryPolicy,
        cron_schedule: Option<String>,
        cron_timezone: Option<String>,
        is_repeating: bool,
        repeat_interval: Option<u32>,
        delay_seconds: u32,
    ) -> Self {
        // Extract retry strategy and intervals from the given retry policy.
        let (retry_strategy, retry_interval, base_interval) = to_retry(retry_policy);
        let repeat_interval = match repeat_interval {
            Some(v) => v,
            None => Default::default(),
        };
        Self {
            id: generate_token!(),
            task_key,
            task_params,
            queue_name,
            updated_at: utc_now!(),
            status: TaskStatus::Scheduled,
            last_error: Default::default(),
            last_run: Default::default(),
            next_run: Default::default(),
            kind,
            stopped_reason: Default::default(),
            success_count: Default::default(),
            failure_count: Default::default(),
            runner_id: Default::default(),
            retry_strategy,
            retry_interval,
            base_interval,
            max_retries: retry_policy.max_retries,
            cron_schedule,
            cron_timezone,
            is_repeating,
            repeat_interval,
            heartbeat_at: Default::default(),
            delay_seconds,
        }
    }

    pub fn retry_policy(&self) -> RetryPolicy {
        let strategy = match self.retry_strategy {
            Retry::Linear => RetryStrategy::Linear {
                interval: self.retry_interval,
            },
            Retry::Exponential => RetryStrategy::Exponential {
                base: self.base_interval,
            },
        };

        RetryPolicy {
            strategy,
            max_retries: self.max_retries,
        }
    }
}
