package task

import (
	"errors"
	"strings"
	"sync"
	"time"
)

// Status represents the current state of a Task
type Status string

// Priority represents the priority level of a Task
type Priority int

const (
	LowPriority Priority = iota
	MediumPriority
	HighPriority
)

type TaskType int

const (
	OneTimeType TaskType = iota
	RecurringType
	IntervalType
)

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

// Task represents a background job to be executed
type Task struct {
	ID           string        // Unique identifier for the task (name + datetime)
	Name         string        // Name of the task
	Func         func() error  // Function to be executed as part of the task
	Status       Status        // Current status of the task
	Schedule     string        // Schedule for the task execution
	Retries      int           // Number of times the task has been retried
	MaxRetry     int           // Maximum number of retries allowed for the task
	CreatedAt    time.Time     // Timestamp when the task was created
	LastRunAt    time.Time     // Timestamp when the task was last executed
	NextRunAt    time.Time     // Timestamp for the next scheduled run of the task
	CompletedAt  time.Time     // Timestamp when the task was completed
	ErrorMessage string        // Error message associated with the task, if any
	mu           sync.RWMutex  // Mutex for concurrent access to task properties
	Priority     Priority      // Priority level of the task
	Dependencies []string      // List of task dependencies (now using string IDs)
	Timeout      time.Duration // Timeout duration for the task execution
	Type         TaskType      // Type of the task
	Interval     time.Duration // Interval for recurring tasks
}

// generate id for the task
func GenerateID(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, " ", "_"))
}

// NewTask creates a new Task with the given parameters
func NewTask(name string, f func() error, schedule string, maxRetry int, taskType TaskType, timeout time.Duration) *Task {
	return &Task{
		ID:        GenerateID(name),
		Name:      name,
		Func:      f,
		Schedule:  schedule,
		MaxRetry:  maxRetry,
		Status:    StatusPending,
		CreatedAt: time.Now(),
		Type:      taskType,
		Interval:  0,
		Timeout:   timeout,
	}
}

// Run executes the task and updates its status
func (t *Task) Run() error {
	t.mu.Lock()
	t.Status = StatusRunning
	t.LastRunAt = time.Now()
	t.Retries++ // Increment retries before running
	t.mu.Unlock()

	err := t.Func()

	t.mu.Lock()
	defer t.mu.Unlock()

	if err != nil {
		t.ErrorMessage = err.Error()
		if t.Retries < t.MaxRetry {
			t.Status = StatusPending
		} else {
			t.Status = StatusFailed
		}
		return err
	}

	t.Status = StatusCompleted
	t.CompletedAt = time.Now()
	return nil
}

// SetInterval sets the interval for recurring tasks
func (t *Task) SetInterval(interval time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Interval = interval
}

// GetStatus returns the current status of the task
func (t *Task) GetStatus() Status {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Status
}

// SetNextRunTime sets the next scheduled run time for the task
func (t *Task) SetNextRunTime(next time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.NextRunAt = next
}

// IsReadyToRun checks if the task is ready to run based on its next run time
func (t *Task) IsReadyToRun() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Status == StatusPending && !t.NextRunAt.IsZero() && time.Now().After(t.NextRunAt)
}

// Reset resets the task to its initial state
func (t *Task) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Status = StatusPending
	t.Retries = 0
	t.ErrorMessage = ""
	t.LastRunAt = time.Time{}
	t.CompletedAt = time.Time{}
}

// Validate checks if the task is valid
func (t *Task) Validate() error {
	if t.ID == "" {
		return errors.New("task ID cannot be empty")
	}
	if t.Func == nil {
		return errors.New("task function cannot be nil")
	}
	return nil
}

// SetStatus sets the status of the task
func (t *Task) SetStatus(status Status) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Status = status
}
