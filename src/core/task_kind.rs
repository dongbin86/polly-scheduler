use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Defines the type of task to be executed.
pub enum TaskKind {
    /// Represents a cron job, which is scheduled to run at specific intervals.
    Cron,

    /// Represents a repeated job that runs at a regular interval.
    Repeat,

    /// Represents a one-time job that runs once and then completes.
    Once,
}

impl std::fmt::Display for TaskKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskKind::Cron => write!(f, "Cron"),
            TaskKind::Repeat => write!(f, "Repeat"),
            TaskKind::Once => write!(f, "Once"),
        }
    }
}
