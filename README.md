# Jobsy

Jobsy is a robust, flexible, and efficient background job processing library for Go applications. It provides a simple yet powerful API for scheduling and managing tasks, supporting various job types and execution patterns.

## Features

- Multiple task types: one-time, recurring, and interval-based tasks
- Flexible scheduling options: immediate execution, scheduled execution, and periodic execution
- Priority-based task execution
- Automatic task retries with exponential backoff
- Concurrent task execution with configurable worker pool
- In-memory storage with extensible storage interface
- Graceful shutdown and task recovery

## Installation

To install Jobsy, use `go get`:

```bash
go get github.com/nex-gen-tech/jobsy
```

## Quick Start

Here's a simple example to get you started with Jobsy:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/nex-gen-tech/jobsy"
)

func main() {
    // Create a new worker
    w := jobsy.NewWorker(&jobsy.WorkerOptions{})

    // Create a context for graceful shutdown
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Start the worker
    go func() {
        if err := w.Start(ctx); err != nil {
            log.Printf("Worker stopped with error: %v", err)
        }
    }()

    // Schedule an immediate task
    _, err := w.RunNow("Immediate Task", func() error {
        fmt.Println("Executing Immediate Task")
        return nil
    })
    if err != nil {
        log.Fatalf("Failed to schedule immediate task: %v", err)
    }

    // Schedule a task to run after 5 seconds
    _, err = w.RunAt("Delayed Task", func() error {
        fmt.Println("Executing Delayed Task")
        return nil
    }, time.Now().Add(5*time.Second))
    if err != nil {
        log.Fatalf("Failed to schedule delayed task: %v", err)
    }

    // Run the worker for 10 seconds
    time.Sleep(10 * time.Second)

    // Graceful shutdown
    cancel()
    w.WaitForShutdown(5 * time.Second)
}
```

## API Reference

### Creating a Worker

```go
w := jobsy.NewWorker(&jobsy.WorkerOptions{
    Concurrency: 5,
    Timeout:     30 * time.Second,
    Priority:    task.MediumPriority,
    MaxRetry:    3,
})
```

### Starting the Worker

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

err := w.Start(ctx)
```

### Scheduling Tasks

1. Run a task immediately:

```go
task, err := w.RunNow("Immediate Task", func() error {
    // Task logic here
    return nil
})
```

2. Schedule a task for future execution:

```go
task, err := w.RunAt("Scheduled Task", func() error {
    // Task logic here
    return nil
}, time.Now().Add(1*time.Hour))
```

3. Schedule a recurring task:

```go
task, err := w.RunEvery("Recurring Task", func() error {
    // Task logic here
    return nil
}, 5*time.Minute)
```

4. Schedule a task with custom options:

```go
task, err := w.Schedule("Custom Task", func() error {
    // Task logic here
    return nil
}, "*/5 * * * *", jobsy.TaskOptions{
    MaxRetry: 5,
    TimeOut:  10 * time.Second,
})
```

### Managing Tasks

1. Get task status:

```go
status, err := w.GetTaskStatus(taskID)
```

2. Get all tasks:

```go
tasks, err := w.GetAllTasks()
```

### Graceful Shutdown

```go
cancel() // Cancel the context
w.WaitForShutdown(5 * time.Second)
```

## Advanced Usage

### Custom Storage

Jobsy uses in-memory storage by default, but you can implement your own storage backend by implementing the `storage.Storage` interface:

```go
type Storage interface {
    SaveTask(t *task.Task) error
    LoadTask(id uuid.UUID) (*task.Task, error)
    LoadAllTasks() ([]*task.Task, error)
    DeleteTask(id uuid.UUID) error
    UpdateTaskStatus(id uuid.UUID, status task.Status) error
}
```

Then, pass your custom storage implementation when creating a new worker:

```go
customStorage := NewCustomStorage()
w := jobsy.NewWorker(&jobsy.WorkerOptions{
    Storage: customStorage,
})
```

### Error Handling and Retries

Jobsy automatically retries failed tasks with an exponential backoff strategy. You can customize the maximum number of retries:

```go
task, err := w.RunNow("Retry Task", func() error {
    // Task logic that might fail
    return errors.New("temporary error")
}, jobsy.TaskOptions{MaxRetry: 5})
```

### Task Priority

You can set task priorities to control execution order:

```go
task.Priority = task.HighPriority
```

## Testing

To run the test suite, use the following command:

```bash
go test ./...
```

## Contributing

Contributions to Jobsy are welcome! Please feel free to submit a Pull Request.

## License

Jobsy is released under the MIT License. See the LICENSE file for details.