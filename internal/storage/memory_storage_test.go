package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

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
		ID:   "Test_Task_20230101120000",
		Name: "Test Task",
	}

	err := storage.SaveTask(task)
	assert.NoError(t, err)
	assert.Len(t, storage.tasks, 1)
	assert.Equal(t, task, storage.tasks[task.ID])
}

func TestLoadTask(t *testing.T) {
	storage := NewMemoryStorage()
	taskID := "Test_Task_20230101120000"
	task := &task.Task{
		ID:   taskID,
		Name: "Test Task",
	}
	storage.tasks[taskID] = task

	loadedTask, err := storage.LoadTask(taskID)
	assert.NoError(t, err)
	assert.Equal(t, task, loadedTask)

	_, err = storage.LoadTask("NonExistent_Task_20230101120000")
	assert.Error(t, err)
	assert.EqualError(t, err, "task not found")
}

func TestLoadAllTasks(t *testing.T) {
	storage := NewMemoryStorage()
	task1 := &task.Task{ID: "Task_1_20230101120000", Name: "Task 1"}
	task2 := &task.Task{ID: "Task_2_20230101120000", Name: "Task 2"}
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
	taskID := "Test_Task_20230101120000"
	task := &task.Task{
		ID:   taskID,
		Name: "Test Task",
	}
	storage.tasks[taskID] = task

	err := storage.DeleteTask(taskID)
	assert.NoError(t, err)
	assert.Empty(t, storage.tasks)

	err = storage.DeleteTask("NonExistent_Task_20230101120000")
	assert.Error(t, err)
	assert.EqualError(t, err, "task not found")
}

func TestUpdateTaskStatus(t *testing.T) {
	storage := NewMemoryStorage()
	taskID := "Test_Task_20230101120000"
	taskRes := &task.Task{
		ID:     taskID,
		Name:   "Test Task",
		Status: task.StatusPending,
	}
	storage.tasks[taskID] = taskRes

	err := storage.UpdateTaskStatus(taskID, task.StatusCompleted)
	assert.NoError(t, err)
	assert.Equal(t, task.StatusCompleted, storage.tasks[taskID].Status)

	err = storage.UpdateTaskStatus("NonExistent_Task_20230101120000", task.StatusCompleted)
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
		go func(i int) {
			defer wg.Done()
			task := &task.Task{
				ID:   fmt.Sprintf("Concurrent_Task_%d_%s", i, time.Now().Format("20060102150405")),
				Name: "Concurrent Task",
			}
			err := storage.SaveTask(task)
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	assert.Len(t, storage.tasks, taskCount)

	// Concurrent LoadTask and UpdateTaskStatus
	tasks, _ := storage.LoadAllTasks()
	wg.Add(len(tasks) * 2)
	for _, tsk := range tasks {
		go func(id string) {
			defer wg.Done()
			_, err := storage.LoadTask(id)
			assert.NoError(t, err)
		}(tsk.ID)

		go func(id string) {
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
	taskID := "Lifecycle_Task_20230101120000"
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
