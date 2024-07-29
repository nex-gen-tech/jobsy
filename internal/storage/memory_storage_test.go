package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nex-gen-tech/jobsy/internal/task"
	"github.com/stretchr/testify/assert"
)

func TestNewMemoryStorage(t *testing.T) {
	storage := NewMemoryStorage()
	assert.NotNil(t, storage)
	assert.NotNil(t, storage.tasks)
	assert.Empty(t, storage.tasks)
}

func TestSaveTask(t *testing.T) {
	storage := NewMemoryStorage()
	task := &task.Task{
		ID:   uuid.New(),
		Name: "Test Task",
	}

	err := storage.SaveTask(task)
	assert.NoError(t, err)
	assert.Len(t, storage.tasks, 1)
	assert.Equal(t, task, storage.tasks[task.ID])
}

func TestLoadTask(t *testing.T) {
	storage := NewMemoryStorage()
	taskID := uuid.New()
	task := &task.Task{
		ID:   taskID,
		Name: "Test Task",
	}
	storage.tasks[taskID] = task

	loadedTask, err := storage.LoadTask(taskID)
	assert.NoError(t, err)
	assert.Equal(t, task, loadedTask)

	_, err = storage.LoadTask(uuid.New())
	assert.Error(t, err)
	assert.EqualError(t, err, "task not found")
}

func TestLoadAllTasks(t *testing.T) {
	storage := NewMemoryStorage()
	task1 := &task.Task{ID: uuid.New(), Name: "Task 1"}
	task2 := &task.Task{ID: uuid.New(), Name: "Task 2"}
	storage.tasks[task1.ID] = task1
	storage.tasks[task2.ID] = task2

	tasks, err := storage.LoadAllTasks()
	assert.NoError(t, err)
	assert.Len(t, tasks, 2)
	assert.Contains(t, tasks, task1)
	assert.Contains(t, tasks, task2)
}

func TestDeleteTask(t *testing.T) {
	storage := NewMemoryStorage()
	taskID := uuid.New()
	task := &task.Task{
		ID:   taskID,
		Name: "Test Task",
	}
	storage.tasks[taskID] = task

	err := storage.DeleteTask(taskID)
	assert.NoError(t, err)
	assert.Empty(t, storage.tasks)

	err = storage.DeleteTask(uuid.New())
	assert.Error(t, err)
	assert.EqualError(t, err, "task not found")
}

func TestUpdateTaskStatus(t *testing.T) {
	storage := NewMemoryStorage()
	taskID := uuid.New()
	taskRes := &task.Task{
		ID:     taskID,
		Name:   "Test Task",
		Status: task.StatusPending,
	}
	storage.tasks[taskID] = taskRes

	err := storage.UpdateTaskStatus(taskID, task.StatusCompleted)
	assert.NoError(t, err)
	assert.Equal(t, task.StatusCompleted, storage.tasks[taskID].Status)

	err = storage.UpdateTaskStatus(uuid.New(), task.StatusCompleted)
	assert.Error(t, err)
	assert.EqualError(t, err, "task not found")
}

func TestConcurrentAccess(t *testing.T) {
	storage := NewMemoryStorage()
	taskCount := 1000

	// Concurrent SaveTask
	var wg sync.WaitGroup
	wg.Add(taskCount)
	for i := 0; i < taskCount; i++ {
		go func() {
			defer wg.Done()
			task := &task.Task{
				ID:   uuid.New(),
				Name: "Concurrent Task",
			}
			err := storage.SaveTask(task)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	assert.Len(t, storage.tasks, taskCount)

	// Concurrent LoadTask and UpdateTaskStatus
	tasks, _ := storage.LoadAllTasks()
	wg.Add(len(tasks) * 2)
	for _, tsk := range tasks {
		go func(id uuid.UUID) {
			defer wg.Done()
			_, err := storage.LoadTask(id)
			assert.NoError(t, err)
		}(tsk.ID)

		go func(id uuid.UUID) {
			defer wg.Done()
			err := storage.UpdateTaskStatus(id, task.StatusCompleted)
			assert.NoError(t, err)
		}(tsk.ID)
	}
	wg.Wait()

	// Verify all tasks are completed
	completedTasks, _ := storage.LoadAllTasks()
	for _, tsk := range completedTasks {
		assert.Equal(t, task.StatusCompleted, tsk.Status)
	}
}

func TestTaskLifecycle(t *testing.T) {
	storage := NewMemoryStorage()

	// Create and save a task
	taskID := uuid.New()
	newTask := &task.Task{
		ID:        taskID,
		Name:      "Lifecycle Task",
		Status:    task.StatusPending,
		CreatedAt: time.Now(),
	}
	err := storage.SaveTask(newTask)
	assert.NoError(t, err)

	// Load and verify the task
	loadedTask, err := storage.LoadTask(taskID)
	assert.NoError(t, err)
	assert.Equal(t, newTask, loadedTask)

	// Update the task status
	err = storage.UpdateTaskStatus(taskID, task.StatusRunning)
	assert.NoError(t, err)

	// Verify the updated status
	updatedTask, err := storage.LoadTask(taskID)
	assert.NoError(t, err)
	assert.Equal(t, task.StatusRunning, updatedTask.Status)

	// Delete the task
	err = storage.DeleteTask(taskID)
	assert.NoError(t, err)

	// Verify the task is deleted
	_, err = storage.LoadTask(taskID)
	assert.Error(t, err)
	assert.EqualError(t, err, "task not found")
}
