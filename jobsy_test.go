package jobsy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nex-gen-tech/jobsy/internal/queue"
	"github.com/nex-gen-tech/jobsy/internal/scheduler"
	"github.com/nex-gen-tech/jobsy/internal/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockStorage is a mock implementation of the storage.Storage interface
type MockStorage struct {
	mock.Mock
}

// Helper function to create a Worker with mocks
func createMockWorker(ms *MockStorage) *Worker {
	return &Worker{
		queue:      queue.NewQueue(),
		scheduler:  scheduler.NewScheduler(),
		dbAdapter:  ms,
		logger:     log.New(log.Writer(), "TEST: ", log.LstdFlags),
		shutdownCh: make(chan struct{}),
	}
}

// Helper function to wait for a function to complete or timeout
func waitWithTimeout(t *testing.T, f func() error, timeout time.Duration) error {
	t.Helper()
	done := make(chan error)
	go func() {
		done <- f()
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return errors.New("operation timed out")
	}
}

func (m *MockStorage) SaveTask(t *task.Task) error {
	args := m.Called(t)
	return args.Error(0)
}

func (m *MockStorage) LoadTask(id uuid.UUID) (*task.Task, error) {
	args := m.Called(id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*task.Task), args.Error(1)
}

func (m *MockStorage) LoadAllTasks() ([]*task.Task, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*task.Task), args.Error(1)
}

func (m *MockStorage) DeleteTask(id uuid.UUID) error {
	args := m.Called(id)
	return args.Error(0)
}

func (m *MockStorage) UpdateTaskStatus(id uuid.UUID, status task.Status) error {
	args := m.Called(id, status)
	return args.Error(0)
}

// MockQueue is a mock implementation of the queue.Queue interface
type MockQueue struct {
	mock.Mock
}

func (m *MockQueue) Enqueue(t *task.Task) {
	m.Called(t)
}

func (m *MockQueue) Dequeue() *task.Task {
	args := m.Called()
	return args.Get(0).(*task.Task)
}

func (m *MockQueue) DequeueChan() <-chan *task.Task {
	args := m.Called()
	return args.Get(0).(chan *task.Task)
}

// MockScheduler is a mock implementation of the scheduler.Scheduler interface
type MockScheduler struct {
	mock.Mock
}

func (m *MockScheduler) AddTask(id uuid.UUID, cronExpr string, task func()) error {
	args := m.Called(id, cronExpr, task)
	return args.Error(0)
}

func (m *MockScheduler) RemoveTask(id uuid.UUID) {
	m.Called(id)
}

func (m *MockScheduler) ScheduleOnce(id uuid.UUID, when time.Time, task func()) {
	m.Called(id, when, task)
}

func (m *MockScheduler) Start(ctx context.Context) {
	m.Called(ctx)
}

func (m *MockScheduler) Stop() {
	m.Called()
}

func (m *MockScheduler) AddTaskWithInterval(id uuid.UUID, interval time.Duration, task func()) error {
	args := m.Called(id, interval, task)
	return args.Error(0)
}

func TestNewWorker(t *testing.T) {
	tests := []struct {
		name string
		opts *WorkerOptions
		want *Worker
	}{
		{
			name: "Default options",
			opts: &WorkerOptions{},
			want: &Worker{
				concurrency: 10,
				timeout:     5 * time.Second,
				priority:    task.LowPriority,
				maxRetry:    3,
			},
		},
		{
			name: "Custom options",
			opts: &WorkerOptions{
				Concurrency: 5,
				Timeout:     2 * time.Minute,
				Priority:    task.HighPriority,
				MaxRetry:    5,
			},
			want: &Worker{
				concurrency: 5,
				timeout:     2 * time.Minute,
				priority:    task.HighPriority,
				maxRetry:    5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWorker(tt.opts)
			assert.NotNil(t, w.queue)
			assert.NotNil(t, w.scheduler)
			assert.NotNil(t, w.dbAdapter)
			assert.Equal(t, tt.want.concurrency, w.concurrency)
			assert.Equal(t, tt.want.timeout, w.timeout)
			assert.Equal(t, tt.want.priority, w.priority)
			assert.Equal(t, tt.want.maxRetry, w.maxRetry)
		})
	}
}

func TestWorker_Start(t *testing.T) {
	tests := []struct {
		name       string
		setupMocks func(*MockStorage)
		wantErr    bool
	}{
		{
			name: "Successful start",
			setupMocks: func(ms *MockStorage) {
				ms.On("LoadAllTasks").Return([]*task.Task{}, nil)
			},
			wantErr: false,
		},
		{
			name: "Start with storage error",
			setupMocks: func(ms *MockStorage) {
				ms.On("LoadAllTasks").Return(nil, errors.New("storage error"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			w := createMockWorker(mockStorage)

			tt.setupMocks(mockStorage)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			err := w.Start(ctx)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestWorker_Schedule(t *testing.T) {
	tests := []struct {
		name       string
		taskName   string
		cronExpr   string
		setupMocks func(*MockStorage, *scheduler.Scheduler)
		wantErr    bool
	}{
		{
			name:     "Valid recurring task",
			taskName: "Recurring Task",
			cronExpr: "*/5 * * * * *",
			setupMocks: func(ms *MockStorage, sch *scheduler.Scheduler) {
				ms.On("SaveTask", mock.AnythingOfType("*task.Task")).Return(nil)
			},
			wantErr: false,
		},
		{
			name:     "Invalid cron expression",
			taskName: "Invalid Task",
			cronExpr: "invalid",
			setupMocks: func(ms *MockStorage, sch *scheduler.Scheduler) {
				ms.On("SaveTask", mock.AnythingOfType("*task.Task")).Return(nil)
			},
			wantErr: true,
		},
		{
			name:     "Storage error",
			taskName: "Storage Error Task",
			cronExpr: "*/5 * * * * *",
			setupMocks: func(ms *MockStorage, sch *scheduler.Scheduler) {
				ms.On("SaveTask", mock.AnythingOfType("*task.Task")).Return(errors.New("storage error"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			sch := scheduler.NewScheduler()

			w := &Worker{
				scheduler: sch,
				dbAdapter: mockStorage,
				queue:     queue.NewQueue(),
			}

			tt.setupMocks(mockStorage, sch)

			_, err := w.Schedule(tt.taskName, func() error { return nil }, tt.cronExpr)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestWorker_RunEvery(t *testing.T) {
	tests := []struct {
		name       string
		taskName   string
		interval   time.Duration
		setupMocks func(*MockStorage)
		wantErr    bool
	}{
		{
			name:     "Valid interval task",
			taskName: "Interval Task",
			interval: 5 * time.Minute,
			setupMocks: func(ms *MockStorage) {
				ms.On("SaveTask", mock.AnythingOfType("*task.Task")).Return(nil)
			},
			wantErr: false,
		},
		{
			name:     "Storage error",
			taskName: "Storage Error Task",
			interval: 5 * time.Minute,
			setupMocks: func(ms *MockStorage) {
				ms.On("SaveTask", mock.AnythingOfType("*task.Task")).Return(errors.New("storage error"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			w := createMockWorker(mockStorage)

			tt.setupMocks(mockStorage)

			_, err := w.RunEvery(tt.taskName, func() error { return nil }, tt.interval)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestWorker_RunNow(t *testing.T) {
	tests := []struct {
		name       string
		taskName   string
		setupMocks func(*MockStorage)
		wantErr    bool
	}{
		{
			name:     "Valid immediate task",
			taskName: "Immediate Task",
			setupMocks: func(ms *MockStorage) {
				ms.On("SaveTask", mock.AnythingOfType("*task.Task")).Return(nil)
			},
			wantErr: false,
		},
		{
			name:     "Storage error",
			taskName: "Storage Error Task",
			setupMocks: func(ms *MockStorage) {
				ms.On("SaveTask", mock.AnythingOfType("*task.Task")).Return(errors.New("storage error"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			w := createMockWorker(mockStorage)

			tt.setupMocks(mockStorage)

			_, err := w.RunNow(tt.taskName, func() error { return nil })

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestWorker_RunAt(t *testing.T) {
	tests := []struct {
		name       string
		taskName   string
		executeAt  time.Time
		setupMocks func(*MockStorage)
		wantErr    bool
	}{
		{
			name:      "Valid scheduled task",
			taskName:  "Scheduled Task",
			executeAt: time.Now().Add(1 * time.Hour),
			setupMocks: func(ms *MockStorage) {
				ms.On("SaveTask", mock.AnythingOfType("*task.Task")).Return(nil)
			},
			wantErr: false,
		},
		{
			name:      "Storage error",
			taskName:  "Storage Error Task",
			executeAt: time.Now().Add(1 * time.Hour),
			setupMocks: func(ms *MockStorage) {
				ms.On("SaveTask", mock.AnythingOfType("*task.Task")).Return(errors.New("storage error"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			w := createMockWorker(mockStorage)

			tt.setupMocks(mockStorage)

			_, err := w.RunAt(tt.taskName, func() error { return nil }, tt.executeAt)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestWorker_GetTaskStatus(t *testing.T) {
	tests := []struct {
		name       string
		taskID     uuid.UUID
		setupMocks func(*MockStorage)
		wantStatus task.Status
		wantErr    bool
		errMsg     string
	}{
		{
			name:   "Existing task",
			taskID: uuid.New(),
			setupMocks: func(ms *MockStorage) {
				ms.On("LoadTask", mock.Anything).Return(&task.Task{Status: task.StatusRunning}, nil)
			},
			wantStatus: task.StatusRunning,
			wantErr:    false,
		},
		{
			name:   "Non-existent task",
			taskID: uuid.New(),
			setupMocks: func(ms *MockStorage) {
				ms.On("LoadTask", mock.Anything).Return(nil, nil)
			},
			wantStatus: "",
			wantErr:    true,
			errMsg:     "task not found",
		},
		{
			name:   "Storage error",
			taskID: uuid.New(),
			setupMocks: func(ms *MockStorage) {
				ms.On("LoadTask", mock.Anything).Return(nil, errors.New("storage error"))
			},
			wantStatus: "",
			wantErr:    true,
			errMsg:     "storage error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			w := createMockWorker(mockStorage)

			tt.setupMocks(mockStorage)

			status, err := w.GetTaskStatus(tt.taskID)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.EqualError(t, err, tt.errMsg)
				}
				assert.Empty(t, status)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantStatus, status)
			}

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestWorker_GetAllTasks(t *testing.T) {
	tests := []struct {
		name       string
		setupMocks func(*MockStorage)
		wantTasks  []*task.Task
		wantErr    bool
		errMsg     string
	}{
		{
			name: "Multiple tasks",
			setupMocks: func(ms *MockStorage) {
				ms.On("LoadAllTasks").Return([]*task.Task{
					{ID: uuid.New(), Name: "Task 1"},
					{ID: uuid.New(), Name: "Task 2"},
				}, nil)
			},
			wantTasks: []*task.Task{
				{ID: uuid.New(), Name: "Task 1"},
				{ID: uuid.New(), Name: "Task 2"},
			},
			wantErr: false,
		},
		{
			name: "No tasks",
			setupMocks: func(ms *MockStorage) {
				ms.On("LoadAllTasks").Return([]*task.Task{}, nil)
			},
			wantTasks: []*task.Task{},
			wantErr:   false,
		},
		{
			name: "Storage error",
			setupMocks: func(ms *MockStorage) {
				ms.On("LoadAllTasks").Return(nil, errors.New("storage error"))
			},
			wantTasks: nil,
			wantErr:   true,
			errMsg:    "storage error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			w := createMockWorker(mockStorage)

			tt.setupMocks(mockStorage)

			tasks, err := w.GetAllTasks()

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.EqualError(t, err, tt.errMsg)
				}
				assert.Nil(t, tasks)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, len(tt.wantTasks), len(tasks))
				// Note: We can't directly compare the tasks because the IDs will be different
				// Instead, we'll check that the number of tasks matches
			}

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestWorker_executeTask(t *testing.T) {
	tests := []struct {
		name       string
		task       *task.Task
		setupMocks func(*MockStorage)
		wantStatus task.Status
	}{
		{
			name: "Successful task execution",
			task: &task.Task{
				ID:   uuid.New(),
				Name: "Successful Task",
				Func: func() error {
					time.Sleep(50 * time.Millisecond) // Reduced sleep time
					fmt.Println("Successful task executed")
					return nil
				},
				Type: task.OneTimeType,
			},
			setupMocks: func(ms *MockStorage) {
				ms.On("UpdateTaskStatus", mock.AnythingOfType("uuid.UUID"), task.StatusCompleted).Return(nil).Once()
			},
			wantStatus: task.StatusCompleted,
		},
		{
			name: "Failed task execution",
			task: &task.Task{
				ID:   uuid.New(),
				Name: "Failed Task",
				Func: func() error {
					time.Sleep(50 * time.Millisecond) // Reduced sleep time
					fmt.Println("Failed task executed")
					return errors.New("task error")
				},
				Type: task.OneTimeType,
			},
			setupMocks: func(ms *MockStorage) {
				ms.On("UpdateTaskStatus", mock.AnythingOfType("uuid.UUID"), task.StatusFailed).Return(nil).Once()
			},
			wantStatus: task.StatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			w := createMockWorker(mockStorage)
			w.timeout = 5 * time.Second // Set timeout to match the new default

			tt.setupMocks(mockStorage)

			ctx := context.Background()
			w.executeTask(ctx, tt.task)

			fmt.Printf("Test '%s': Expected status %s, got %s\n", tt.name, tt.wantStatus, tt.task.GetStatus())
			assert.Equal(t, tt.wantStatus, tt.task.GetStatus(), "Task status mismatch")

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestWorker_handleTaskTimeout(t *testing.T) {
	tests := []struct {
		name       string
		task       *task.Task
		setupMocks func(*MockStorage, *MockScheduler)
		wantStatus task.Status
	}{
		{
			name: "One-time task timeout",
			task: &task.Task{
				ID:   uuid.New(),
				Name: "One-time Task",
				Type: task.OneTimeType,
			},
			setupMocks: func(ms *MockStorage, msch *MockScheduler) {
				ms.On("UpdateTaskStatus", mock.Anything, task.StatusFailed).Return(nil)
			},
			wantStatus: task.StatusFailed,
		},
		{
			name: "Recurring task timeout",
			task: &task.Task{
				ID:       uuid.New(),
				Name:     "Recurring Task",
				Type:     task.RecurringType,
				Retries:  2,
				MaxRetry: 3,
			},
			setupMocks: func(ms *MockStorage, msch *MockScheduler) {
				ms.On("UpdateTaskStatus", mock.Anything, task.StatusFailed).Return(nil)
				ms.On("SaveTask", mock.Anything).Return(nil)
				msch.On("ScheduleOnce", mock.Anything, mock.Anything, mock.Anything).Return()
			},
			wantStatus: task.StatusPending,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			mockScheduler := new(MockScheduler)

			w := &Worker{
				dbAdapter: mockStorage,
				scheduler: mockScheduler,
				logger:    log.New(os.Stdout, "TEST: ", log.LstdFlags),
				queue:     queue.NewQueue(),
			}

			tt.setupMocks(mockStorage, mockScheduler)

			w.handleTaskTimeout(tt.task)

			t.Logf("Task status after handleTaskTimeout: %s", tt.task.GetStatus())
			assert.Equal(t, tt.wantStatus, tt.task.GetStatus())
			assert.Equal(t, "Task timed out", tt.task.ErrorMessage)

			mockStorage.AssertExpectations(t)
			mockScheduler.AssertExpectations(t)
		})
	}
}

func TestWorker_rescheduleTask(t *testing.T) {
	tests := []struct {
		name       string
		task       *task.Task
		setupMocks func(*MockStorage, *MockScheduler)
		wantStatus task.Status
	}{
		{
			name: "Reschedule within retry limit",
			task: &task.Task{
				ID:       uuid.New(),
				Name:     "Retryable Task",
				Retries:  1,
				MaxRetry: 3,
			},
			setupMocks: func(ms *MockStorage, msch *MockScheduler) {
				ms.On("SaveTask", mock.Anything).Return(nil)
				msch.On("ScheduleOnce", mock.Anything, mock.Anything, mock.Anything).Return()
			},
			wantStatus: task.StatusPending,
		},
		{
			name: "Exceed retry limit",
			task: &task.Task{
				ID:       uuid.New(),
				Name:     "Max Retries Task",
				Retries:  3,
				MaxRetry: 3,
			},
			setupMocks: func(ms *MockStorage, msch *MockScheduler) {
				ms.On("UpdateTaskStatus", mock.Anything, task.StatusFailed).Return(nil)
			},
			wantStatus: task.StatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			mockScheduler := new(MockScheduler)

			w := &Worker{
				dbAdapter: mockStorage,
				scheduler: mockScheduler,
				logger:    log.New(os.Stdout, "TEST: ", log.LstdFlags),
				queue:     queue.NewQueue(),
			}

			tt.setupMocks(mockStorage, mockScheduler)

			w.rescheduleTask(tt.task)

			t.Logf("Task status after rescheduleTask: %s", tt.task.GetStatus())
			assert.Equal(t, tt.wantStatus, tt.task.GetStatus())

			mockStorage.AssertExpectations(t)
			mockScheduler.AssertExpectations(t)
		})
	}
}

func TestWorker_calculateBackoff(t *testing.T) {
	w := NewWorker(&WorkerOptions{})

	tests := []struct {
		name    string
		retries int
		want    time.Duration
	}{
		{"First retry", 1, 1 * time.Second},
		{"Second retry", 2, 2 * time.Second},
		{"Third retry", 3, 4 * time.Second},
		{"Fourth retry", 4, 8 * time.Second},
		{"Max backoff", 20, 1 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := w.calculateBackoff(tt.retries)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWorker_WaitForShutdown(t *testing.T) {
	w := NewWorker(&WorkerOptions{})

	tests := []struct {
		name    string
		timeout time.Duration
		want    bool
	}{
		{"Shutdown before timeout", 100 * time.Millisecond, true},
		{"Timeout before shutdown", 10 * time.Millisecond, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w.shutdownCh = make(chan struct{})

			if tt.want {
				go func() {
					time.Sleep(50 * time.Millisecond)
					close(w.shutdownCh)
				}()
			}

			got := w.WaitForShutdown(tt.timeout)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWorker_Reset(t *testing.T) {
	w := NewWorker(&WorkerOptions{})
	w.isRunning = true

	oldQueue := w.queue
	oldScheduler := w.scheduler
	oldShutdownCh := w.shutdownCh

	done := make(chan struct{})
	go func() {
		w.Reset()
		close(done)
	}()

	select {
	case <-done:
		// Reset completed successfully
	case <-time.After(1 * time.Second):
		t.Fatal("Reset timed out")
	}

	assert.False(t, w.isRunning)
	assert.NotSame(t, oldQueue, w.queue)
	assert.NotSame(t, oldScheduler, w.scheduler)
	assert.NotSame(t, oldShutdownCh, w.shutdownCh)
}
