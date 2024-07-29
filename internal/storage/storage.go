package storage

import (
	"github.com/google/uuid"
	"github.com/nex-gen-tech/jobsy/internal/task"
)

// Storage defines the interface for database operations
// Storage is an interface that defines methods for interacting with a storage system.
type Storage interface {
	// SaveTask saves the given task to the storage.
	SaveTask(t *task.Task) error

	// LoadTask loads the task with the specified ID from the storage.
	// It returns the loaded task and an error if the task is not found.
	LoadTask(id uuid.UUID) (*task.Task, error)

	// LoadAllTasks loads all tasks from the storage.
	// It returns a slice of loaded tasks and an error if any.
	LoadAllTasks() ([]*task.Task, error)

	// DeleteTask deletes the task with the specified ID from the storage.
	// It returns an error if the task is not found or if there is an issue with deletion.
	DeleteTask(id uuid.UUID) error

	// UpdateTaskStatus updates the status of the task with the specified ID in the storage.
	// It returns an error if the task is not found or if there is an issue with updating the status.
	UpdateTaskStatus(id uuid.UUID, status task.Status) error
}
