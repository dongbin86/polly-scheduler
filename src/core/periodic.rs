use crate::core::error::BoxError;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::signal;
use tokio::{sync::Notify, time::sleep};
use tracing::{error, info, warn};

#[derive(Default)]
pub struct PeriodicTask {
    // Name of the periodic task.
    name: String,
    // A notification mechanism for shutdown signaling.
    shutdown: Arc<Notify>,
}

impl PeriodicTask {
    /// Creates a new instance of `PeriodicTask`.
    ///
    /// # Arguments
    ///
    /// * `name`: A string slice that holds the name of the task.
    ///
    /// # Returns
    ///
    /// Returns a new instance of `PeriodicTask`.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Sends a shutdown signal to the task.
    ///
    /// This method notifies the task to stop executing.
    pub fn shutdown(self: Arc<Self>) {
        self.shutdown.notify_one();
    }

    /// Starts the periodic task and sets up a signal handler for shutdown.
    ///
    /// # Arguments
    ///
    /// * `task`: An `Arc` of a function that returns a future to be executed periodically.
    /// * `interval`: A `Duration` specifying how often the task should run.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the future returned by the task function.
    ///
    /// # Requirements
    ///
    /// The future must output a `Result` with a unit value or a `BoxError` on failure.
    pub fn start_with_signal<F>(
        self: Arc<Self>,
        task: Arc<dyn Fn() -> F + Send + Sync>,
        interval: Duration,
    ) where
        F: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        // Clone the periodic task instance for the task runner.
        let task_clone = Arc::clone(&self);
        let task_runner = async move {
            // Run the task periodically.
            task_clone.run(task.clone(), interval).await;
        };

        // Clone the periodic task instance for the signal handler.
        let signal_clone = Arc::clone(&self);
        let signal_handler = async move {
            // Listen for a shutdown signal (Ctrl+C).
            match signal::ctrl_c().await {
                Ok(()) => {
                    info!(
                        "Received SIGINT (Ctrl+C), shutting down periodic task '{}'...",
                        &self.name
                    );
                    // Notify the task to shut down.
                    signal_clone.shutdown();
                }
                Err(err) => {
                    error!(
                        "Error listening for shutdown signal: {:?}",
                        BoxError::from(err)
                    );
                }
            }
        };

        // Spawn the task runner and signal handler as asynchronous tasks.
        tokio::spawn(task_runner);
        tokio::spawn(signal_handler);
    }

    /// Runs the periodic task at the specified interval.
    ///
    /// # Arguments
    ///
    /// * `task`: An `Arc` of a function that returns a future to be executed.
    /// * `interval`: A `Duration` specifying how often the task should run.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the future returned by the task function.
    async fn run<F>(self: Arc<Self>, task: Arc<dyn Fn() -> F + Send + Sync>, interval: Duration)
    where
        F: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        info!("Periodic task '{}' started", &self.name);
        loop {
            tokio::select! {
                // Wait for a shutdown notification.
                _ = self.shutdown.notified() => {
                    info!("Received shutdown signal, stopping periodic task '{}'.", &self.name);
                    break; // Exit the loop to stop the task.
                }
                // Wait for the specified interval to elapse.
                _ = sleep(interval) => {
                    // Clone the task to execute it.
                    let task_clone = Arc::clone(&task);
                    let task_future = tokio::spawn(async move {
                        task_clone().await // Execute the task.
                    });

                    // Handle the result of the task execution.
                    match task_future.await {
                        Ok(Ok(_)) => {
                            info!("Periodic task '{}' completed successfully.", &self.name);
                        }
                        Ok(Err(e)) => {
                            warn!("Periodic task '{}' failed: {:?}", &self.name, e);
                        }
                        Err(e) if e.is_panic() => {
                            error!("Fatal: Periodic task '{}' encountered a panic.", &self.name);
                        }
                        Err(e) => {
                            error!("Periodic task '{}' failed unexpectedly: {:?}", &self.name, e);
                        }
                    }
                }
            }
        }
        info!("Periodic task '{}' stopped", &self.name);
    }
}
