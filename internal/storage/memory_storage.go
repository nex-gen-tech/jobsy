package storage

import (
	"errors"
	"sync"

	"github.com/nex-gen-tech/jobsy/internal/task"
)

// MemoryStorage is an in-memory implementation of the Storage interface.
type MemoryStorage struct {
	tasks map[string]*task.Task
	mu    sync.RWMutex
}

// NewMemoryStorage creates and returns a new instance of MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		tasks: make(map[string]*task.Task),
	}
}

// SaveTask saves the given task to the in-memory storage.
func (m *MemoryStorage) SaveTask(t *task.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[t.ID] = t
	return nil
}

// LoadTask loads the task with the specified ID from the in-memory storage.
func (m *MemoryStorage) LoadTask(id string) (*task.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tasks[id]
	if !ok {
		return nil, errors.New("task not found")
	}
	return t, nil
}

// LoadAllTasks loads all tasks from the in-memory storage.
func (m *MemoryStorage) LoadAllTasks() ([]*task.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tasks := make([]*task.Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		tasks = append(tasks, t)
	}
	return tasks, nil
}

// DeleteTask deletes the task with the specified ID from the in-memory storage.
func (m *MemoryStorage) DeleteTask(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tasks[id]; !ok {
		return errors.New("task not found")
	}
	delete(m.tasks, id)
	return nil
}

// UpdateTaskStatus updates the status of the task with the specified ID in the in-memory storage.
func (m *MemoryStorage) UpdateTaskStatus(id string, status task.Status) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return errors.New("task not found")
	}
	t.SetStatus(status)
	return nil
}
