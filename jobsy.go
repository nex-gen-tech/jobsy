package jobsy

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nex-gen-tech/jobsy/internal/queue"
	"github.com/nex-gen-tech/jobsy/internal/scheduler"
	"github.com/nex-gen-tech/jobsy/internal/storage"
	"github.com/nex-gen-tech/jobsy/internal/task"
)

// Worker manages the execution of background tasks
// Worker represents a worker that processes jobs from a queue.
type Worker struct {
	queue           *queue.Queue                 // The queue from which the worker fetches jobs.
	scheduler       scheduler.SchedulerInterface // The scheduler used by the worker to schedule jobs.
	dbAdapter       storage.Storage              // The database adapter used by the worker.
	concurrency     int                          // The maximum number of concurrent jobs that the worker can process.
	logger          *log.Logger                  // The logger used by the worker to log messages.
	wg              sync.WaitGroup               // WaitGroup to wait for all jobs to finish.
	mu              sync.RWMutex                 // Mutex to synchronize access to the worker's state.
	isRunning       bool                         // Flag indicating whether the worker is currently running.
	shutdownCh      chan struct{}                // Channel used to signal the worker to shutdown.
	shutdownOnce    sync.Once                    // Once to ensure shutdown is only executed once.
	debugCh         chan string                  // Channel used to receive debug messages from the worker.
	tasksInProgress sync.Map                     // Map to store tasks in progress
	taskQueues      map[uuid.UUID]chan struct{}  // Map to store task queues
	queueMu         sync.Mutex                   // Mutex to synchronize access to task queues
	priority        task.Priority                // Default priority for tasks
	timeout         time.Duration                // Default timeout for tasks
	maxRetry        int                          // Default maximum number of retries for tasks
	taskChan        chan interface{}             // Channel used to send tasks to the worker
}

// WorkerOptions represents the options for creating a new Worker instance
type WorkerOptions struct {
	Concurrency int
	Storage     storage.Storage
	Logger      *log.Logger
	Timeout     time.Duration
	Priority    task.Priority
	MaxRetry    int
}

// NewWorker creates a new Worker instance
func NewWorker(opts *WorkerOptions) *Worker {
	if opts == nil {
		opts = &WorkerOptions{}
	}

	concurrency := 10
	if opts.Concurrency > 0 {
		concurrency = opts.Concurrency
	}

	timeout := 5 * time.Second // Reduced default timeout
	if opts.Timeout > 0 {
		timeout = opts.Timeout
	}

	priority := task.LowPriority
	if opts.Priority != 0 {
		priority = opts.Priority
	}

	maxRetry := 3
	if opts.MaxRetry > 0 {
		maxRetry = opts.MaxRetry
	}

	storageNew := opts.Storage
	if storageNew == nil {
		storageNew = storage.NewMemoryStorage()
	}

	return &Worker{
		queue:           queue.NewQueue(),
		scheduler:       scheduler.NewScheduler(),
		dbAdapter:       storageNew,
		concurrency:     concurrency,
		logger:          log.New(log.Writer(), "JOBSY: ", log.LstdFlags),
		shutdownCh:      make(chan struct{}),
		debugCh:         make(chan string, 100),
		tasksInProgress: sync.Map{},
		taskQueues:      make(map[uuid.UUID]chan struct{}),
		priority:        priority,
		timeout:         timeout,
		maxRetry:        maxRetry,
		taskChan:        make(chan interface{}, 1000),
	}
}

// Start starts the worker and begins executing tasks
func (w *Worker) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.isRunning {
		w.mu.Unlock()
		return errors.New("worker is already running")
	}
	w.isRunning = true
	w.mu.Unlock()

	w.Reset() // Reset the worker state before starting

	errChan := make(chan error, 1)
	readyChan := make(chan struct{})

	go func() {
		defer close(errChan)
		defer w.shutdown()

		if err := w.loadTasks(); err != nil {
			errChan <- err
			return
		}

		// Start the task dispatcher
		go w.taskDispatcher(ctx)

		// Start worker goroutines
		for i := 0; i < w.concurrency; i++ {
			w.wg.Add(1)
			go w.runWorker(ctx)
		}

		w.scheduler.Start(ctx)

		// Signal that the worker is ready
		close(readyChan)

		<-ctx.Done()
		w.logger.Println("Context done, shutting down worker...")
		errChan <- ctx.Err()
	}()

	select {
	case <-readyChan:
		w.logger.Println("Worker started successfully")
		return nil
	case err := <-errChan:
		return err
	case <-ctx.Done():
		w.shutdown()
		return ctx.Err()
	}
}

func (w *Worker) taskDispatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-w.taskChan:
			w.queue.Enqueue(task)
		}
	}
}

// runWorker is a goroutine that continuously dequeues and executes tasks
func (w *Worker) runWorker(ctx context.Context) {
	defer w.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-w.queue.DequeueChan():
			if t != nil {
				if task, ok := t.(*task.Task); ok {
					w.executeTask(ctx, task)
				} else {
					w.logger.Printf("Received invalid task type: %T", t)
				}
			}
		case <-time.After(100 * time.Millisecond):
			// No task available, continue loop
		}
	}
}

// executeTask executes a task and handles its completion or timeout
func (w *Worker) executeTask(ctx context.Context, t *task.Task) {
	// Check if the task is already being executed
	if _, loaded := w.tasksInProgress.LoadOrStore(t.ID, true); loaded {
		return
	}
	defer w.tasksInProgress.Delete(t.ID)

	timeOut := w.timeout
	if t.Timeout > 0 {
		timeOut = t.Timeout
	}

	taskCtx, cancel := context.WithTimeout(ctx, timeOut)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		err := t.Run()
		done <- err
	}()

	var finalStatus task.Status
	select {
	case err := <-done:
		if err != nil {
			t.ErrorMessage = err.Error()
			finalStatus = task.StatusFailed
			if t.Type != task.OneTimeType {
				w.rescheduleTask(t)
			}
		} else {
			finalStatus = task.StatusCompleted
		}
	case <-taskCtx.Done():
		w.handleTaskTimeout(t)
	}

	t.SetStatus(finalStatus)

	// Update the task status in the database
	if err := w.dbAdapter.UpdateTaskStatus(t.ID, finalStatus); err != nil {
		w.logger.Printf("Failed to update task status for %s (%s): %v", t.Name, t.ID, err)
	}

	// For one-time tasks, ensure it's removed from the scheduler
	if t.Type == task.OneTimeType {
		w.scheduler.RemoveTask(t.ID)
	}
}

// checkDependencies checks if all the dependencies of a task are met
func (w *Worker) checkDependencies(t *task.Task) bool {
	for _, depID := range t.Dependencies {
		depTask, err := w.dbAdapter.LoadTask(depID)
		if err != nil || depTask.GetStatus() != task.StatusCompleted {
			return false
		}
	}
	return true
}

// handleTaskCompletion handles the completion of a task
func (w *Worker) handleTaskCompletion(t *task.Task, err error) {
	if err != nil {
		t.ErrorMessage = err.Error()
		if t.GetStatus() == task.StatusPending {
			w.rescheduleTask(t)
		}
	} else {
		t.SetStatus(task.StatusCompleted)
	}
}

// handleTaskTimeout handles the timeout of a task
func (w *Worker) handleTaskTimeout(t *task.Task) {
	t.SetStatus(task.StatusFailed)
	t.ErrorMessage = "Task timed out"

	if err := w.dbAdapter.UpdateTaskStatus(t.ID, task.StatusFailed); err != nil {
		w.logger.Printf("Failed to update task status for %s (%s): %v", t.Name, t.ID, err)
	}

	if t.Type != task.OneTimeType {
		w.rescheduleTask(t)
	}
}

// rescheduleTask reschedules a task for execution in the future
func (w *Worker) rescheduleTask(t *task.Task) {
	t.Retries++
	if t.Retries > t.MaxRetry {
		t.SetStatus(task.StatusFailed)
		if err := w.dbAdapter.UpdateTaskStatus(t.ID, task.StatusFailed); err != nil {
			w.logger.Printf("Error updating task status for %s (%s): %v", t.Name, t.ID, err)
		}
		return
	}

	t.SetStatus(task.StatusPending)
	backoff := w.calculateBackoff(t.Retries)
	nextRunTime := time.Now().Add(backoff)
	t.SetNextRunTime(nextRunTime)

	if err := w.dbAdapter.SaveTask(t); err != nil {
		w.logger.Printf("Error saving rescheduled task %s (%s): %v", t.Name, t.ID, err)
		return
	}

	w.scheduler.ScheduleOnce(t.ID, nextRunTime, func() {
		w.queue.Enqueue(t)
	})
}

// calculateBackoff calculates the backoff duration for retrying a task
func (w *Worker) calculateBackoff(retries int) time.Duration {
	backoff := time.Duration(1<<uint(retries-1)) * time.Second
	if backoff > 1*time.Hour {
		backoff = 1 * time.Hour // Cap the delay at 1 hour
	}
	return backoff
}

// TaskOptions represents the options for adding a new task
type TaskOptions struct {
	MaxRetry int
	TimeOut  time.Duration
}

// Schedule adds a new task to the worker
func (w *Worker) Schedule(name string, f func() error, schedule string, opts ...TaskOptions) (*task.Task, error) {
	id := uuid.New()
	taskType := task.RecurringType
	if schedule == "" {
		taskType = task.OneTimeType
	}

	maxRetry := w.maxRetry
	if len(opts) > 0 && opts[0].MaxRetry != 0 {
		maxRetry = opts[0].MaxRetry
	}
	timeOut := w.timeout
	if len(opts) > 0 && opts[0].TimeOut != 0 {
		timeOut = opts[0].TimeOut
	}

	t := task.NewTask(id, name, f, schedule, maxRetry, taskType, timeOut)

	if err := t.Validate(); err != nil {
		return nil, err
	}

	if err := w.dbAdapter.SaveTask(t); err != nil {
		return nil, err
	}

	if schedule != "" {
		err := w.scheduler.AddTask(t.ID, t.Schedule, func() {
			w.queue.Enqueue(t)
		})
		if err != nil {
			return nil, err
		}
	} else {
		// Use a non-blocking send to check if the queue is full or closed
		select {
		case w.taskChan <- t:
			// Task enqueued successfully
		default:
			return nil, errors.New("failed to enqueue task: queue is full or closed")
		}
	}

	return t, nil
}

// RunEvery schedules a task to run at regular intervals
func (w *Worker) RunEvery(name string, f func() error, interval time.Duration, opts ...TaskOptions) (*task.Task, error) {
	id := uuid.New()

	maxRetry := w.maxRetry
	if len(opts) > 0 && opts[0].MaxRetry != 0 {
		maxRetry = opts[0].MaxRetry
	}
	timeOut := w.timeout
	if len(opts) > 0 && opts[0].TimeOut != 0 {
		timeOut = opts[0].TimeOut
	}

	t := task.NewTask(id, name, f, "", maxRetry, task.IntervalType, timeOut)
	t.SetInterval(interval)

	if err := w.dbAdapter.SaveTask(t); err != nil {
		return nil, err
	}

	err := w.scheduler.AddTaskWithInterval(t.ID, interval, func() {
		w.queue.Enqueue(t)
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// RunNow runs a task immediately
func (w *Worker) RunNow(name string, f func() error, opts ...TaskOptions) (*task.Task, error) {
	id := uuid.New()

	maxRetry := w.maxRetry
	if len(opts) > 0 && opts[0].MaxRetry != 0 {
		maxRetry = opts[0].MaxRetry
	}
	timeOut := w.timeout
	if len(opts) > 0 && opts[0].TimeOut != 0 {
		timeOut = opts[0].TimeOut
	}

	t := task.NewTask(id, name, f, "", maxRetry, task.OneTimeType, timeOut)

	if err := t.Validate(); err != nil {
		return nil, err
	}

	if err := w.dbAdapter.SaveTask(t); err != nil {
		return nil, err
	}

	w.queue.Enqueue(t)

	return t, nil
}

// RunAt runs a task at a specific time once
func (w *Worker) RunAt(name string, f func() error, executeAt time.Time, opts ...TaskOptions) (*task.Task, error) {
	id := uuid.New()

	maxRetry := w.maxRetry
	if len(opts) > 0 && opts[0].MaxRetry != 0 {
		maxRetry = opts[0].MaxRetry
	}
	timeOut := w.timeout
	if len(opts) > 0 && opts[0].TimeOut != 0 {
		timeOut = opts[0].TimeOut
	}

	t := task.NewTask(id, name, f, "", maxRetry, task.OneTimeType, timeOut)
	t.SetNextRunTime(executeAt)

	if err := w.dbAdapter.SaveTask(t); err != nil {
		return nil, err
	}

	w.scheduler.ScheduleOnce(t.ID, executeAt, func() {
		w.queue.Enqueue(t)
	})

	return t, nil
}

// GetTaskStatus returns the status of a task
func (w *Worker) GetTaskStatus(id uuid.UUID) (task.Status, error) {
	t, err := w.dbAdapter.LoadTask(id)
	if err != nil {
		return "", err
	}
	if t == nil {
		return "", errors.New("task not found")
	}
	return t.GetStatus(), nil
}

// GetAllTasks returns all tasks
func (w *Worker) GetAllTasks() ([]*task.Task, error) {
	return w.dbAdapter.LoadAllTasks()
}

// loadTasks loads tasks from the database and schedules them for execution
func (w *Worker) loadTasks() error {
	tasks, err := w.dbAdapter.LoadAllTasks()
	if err != nil {
		return err
	}

	for _, t := range tasks {
		switch t.Type {
		case task.RecurringType:
			w.scheduler.AddTask(t.ID, t.Schedule, func() {
				w.queue.Enqueue(t)
			})
		case task.IntervalType:
			w.scheduler.AddTaskWithInterval(t.ID, t.Interval, func() {
				w.queue.Enqueue(t)
			})
		case task.OneTimeType:
			if t.NextRunAt.After(time.Now()) {
				w.scheduler.ScheduleOnce(t.ID, t.NextRunAt, func() {
					w.queue.Enqueue(t)
				})
			} else if t.GetStatus() == task.StatusPending {
				w.queue.Enqueue(t)
			}
		}
	}

	return nil
}

// shutdown gracefully shuts down the worker
func (w *Worker) shutdown() {
	w.mu.Lock()
	if !w.isRunning {
		w.mu.Unlock()
		return
	}
	w.isRunning = false
	w.mu.Unlock()

	w.scheduler.Stop()
	w.queue.Close()

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		w.logger.Println("Shutdown timed out, some goroutines may not have finished")
	}

	close(w.shutdownCh)
}

// WaitForShutdown waits for the worker to shut down with a timeout
func (w *Worker) WaitForShutdown(timeout time.Duration) bool {
	select {
	case <-w.shutdownCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Reset resets the worker to its initial state
func (w *Worker) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.queue != nil {
		w.queue.Close()
	}
	if w.scheduler != nil {
		w.scheduler.Stop()
	}

	w.isRunning = false
	w.queue = queue.NewQueue()
	w.scheduler = scheduler.NewScheduler()
	w.shutdownCh = make(chan struct{})
	w.shutdownOnce = sync.Once{}
	w.tasksInProgress = sync.Map{}
	w.taskQueues = make(map[uuid.UUID]chan struct{})

	// Use a very short timeout when waiting for ongoing operations
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All operations completed
	case <-time.After(100 * time.Millisecond):
		w.logger.Println("Reset timed out waiting for ongoing operations")
	}
}
