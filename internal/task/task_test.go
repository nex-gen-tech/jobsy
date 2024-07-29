package task

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewTask(t *testing.T) {
	id := uuid.New()
	name := "Test Task"
	f := func() error { return nil }
	schedule := "* * * * *"
	maxRetry := 3
	taskType := OneTimeType
	timeout := 1 * time.Minute

	task := NewTask(id, name, f, schedule, maxRetry, taskType, timeout)

	assert.Equal(t, id, task.ID)
	assert.Equal(t, name, task.Name)
	assert.NotNil(t, task.Func)
	assert.Equal(t, schedule, task.Schedule)
	assert.Equal(t, maxRetry, task.MaxRetry)
	assert.Equal(t, StatusPending, task.Status)
	assert.Equal(t, taskType, task.Type)
	assert.Equal(t, timeout, task.Timeout)
	assert.NotZero(t, task.CreatedAt)
}

func TestTask_Run(t *testing.T) {
	t.Run("Successful Run", func(t *testing.T) {
		task := NewTask(uuid.New(), "Test Task", func() error { return nil }, "", 3, OneTimeType, time.Minute)
		err := task.Run()

		assert.NoError(t, err)
		assert.Equal(t, StatusCompleted, task.Status)
		assert.Equal(t, 1, task.Retries)
		assert.NotZero(t, task.LastRunAt)
		assert.NotZero(t, task.CompletedAt)
	})

	t.Run("Failed Run", func(t *testing.T) {
		task := NewTask(uuid.New(), "Test Task", func() error { return errors.New("test error") }, "", 3, OneTimeType, time.Minute)
		err := task.Run()

		assert.Error(t, err)
		assert.Equal(t, StatusPending, task.Status)
		assert.Equal(t, 1, task.Retries)
		assert.NotZero(t, task.LastRunAt)
		assert.Equal(t, "test error", task.ErrorMessage)
	})

	t.Run("Max Retries Reached", func(t *testing.T) {
		task := NewTask(uuid.New(), "Test Task", func() error { return errors.New("test error") }, "", 1, OneTimeType, time.Minute)
		_ = task.Run()
		err := task.Run()

		assert.Error(t, err)
		assert.Equal(t, StatusFailed, task.Status)
		assert.Equal(t, 2, task.Retries)
	})
}

func TestTask_SetInterval(t *testing.T) {
	task := NewTask(uuid.New(), "Test Task", func() error { return nil }, "", 3, IntervalType, time.Minute)
	interval := 5 * time.Minute
	task.SetInterval(interval)

	assert.Equal(t, interval, task.Interval)
}

func TestTask_GetStatus(t *testing.T) {
	task := NewTask(uuid.New(), "Test Task", func() error { return nil }, "", 3, OneTimeType, time.Minute)
	assert.Equal(t, StatusPending, task.GetStatus())

	task.SetStatus(StatusRunning)
	assert.Equal(t, StatusRunning, task.GetStatus())
}

func TestTask_SetNextRunTime(t *testing.T) {
	task := NewTask(uuid.New(), "Test Task", func() error { return nil }, "", 3, OneTimeType, time.Minute)
	nextRun := time.Now().Add(1 * time.Hour)
	task.SetNextRunTime(nextRun)

	assert.Equal(t, nextRun, task.NextRunAt)
}

func TestTask_IsReadyToRun(t *testing.T) {
	task := NewTask(uuid.New(), "Test Task", func() error { return nil }, "", 3, OneTimeType, time.Minute)

	assert.False(t, task.IsReadyToRun(), "Task should not be ready to run with zero NextRunAt")

	task.SetNextRunTime(time.Now().Add(-1 * time.Minute))
	assert.True(t, task.IsReadyToRun(), "Task should be ready to run")

	task.SetNextRunTime(time.Now().Add(1 * time.Minute))
	assert.False(t, task.IsReadyToRun(), "Task should not be ready to run")

	task.SetStatus(StatusRunning)
	assert.False(t, task.IsReadyToRun(), "Running task should not be ready to run")
}

func TestTask_Reset(t *testing.T) {
	task := NewTask(uuid.New(), "Test Task", func() error { return nil }, "", 3, OneTimeType, time.Minute)
	task.Status = StatusCompleted
	task.Retries = 2
	task.ErrorMessage = "test error"
	task.LastRunAt = time.Now()
	task.CompletedAt = time.Now()

	task.Reset()

	assert.Equal(t, StatusPending, task.Status)
	assert.Equal(t, 0, task.Retries)
	assert.Empty(t, task.ErrorMessage)
	assert.True(t, task.LastRunAt.IsZero())
	assert.True(t, task.CompletedAt.IsZero())
}

func TestTask_Validate(t *testing.T) {
	t.Run("Valid Task", func(t *testing.T) {
		task := NewTask(uuid.New(), "Test Task", func() error { return nil }, "", 3, OneTimeType, time.Minute)
		err := task.Validate()
		assert.NoError(t, err)
	})

	t.Run("Invalid ID", func(t *testing.T) {
		task := NewTask(uuid.Nil, "Test Task", func() error { return nil }, "", 3, OneTimeType, time.Minute)
		err := task.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "task ID cannot be empty")
	})

	t.Run("Nil Function", func(t *testing.T) {
		task := NewTask(uuid.New(), "Test Task", nil, "", 3, OneTimeType, time.Minute)
		err := task.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "task function cannot be nil")
	})
}

func TestTask_SetStatus(t *testing.T) {
	task := NewTask(uuid.New(), "Test Task", func() error { return nil }, "", 3, OneTimeType, time.Minute)
	task.SetStatus(StatusRunning)
	assert.Equal(t, StatusRunning, task.Status)

	task.SetStatus(StatusCompleted)
	assert.Equal(t, StatusCompleted, task.Status)
}
