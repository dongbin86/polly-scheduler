# Polly Scheduler

**Polly Scheduler** is an asynchronous task scheduling system built on the Tokio runtime, designed for efficiency and simplicity. It supports a variety of scheduling options, including Cron, repeating tasks, and one-time executions. The scheduler features robust support for retrying failed tasks, ensuring reliable execution even in the face of transient errors.

## Key Functionalities

- **Task State Persistence:** Maintain task states across application restarts, ensuring continuity and reliability.
- **Task Queues:** Efficiently manage tasks through dedicated queues, optimizing processing and execution order.
- **Process Restart Recovery:** Automatically recover tasks that were in progress at the time of a process restart, minimizing disruptions.
- **Graceful Shutdown:** Support for clean and controlled shutdowns, allowing tasks to complete before termination.
- **Crash Recovery:** Capture crashes during task execution, preventing application exits and ensuring continued operation.

Inspired by the **background-jobs** library (version 0.19.0), Polly Scheduler offers a more streamlined approach, focusing on simplicity and ease of use. Currently, it supports Native DB as its persistent storage backend, making it a robust choice for developers seeking a reliable task scheduling solution.

Whether you need to run periodic tasks or manage complex job workflows, Polly Scheduler provides the features and flexibility you need to keep your applications running smoothly.
