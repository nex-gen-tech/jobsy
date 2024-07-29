package queue

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nex-gen-tech/jobsy/internal/task"
	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	t.Run("EnqueueAndDequeue", func(t *testing.T) {
		q := NewQueue()
		task1 := createTask("task1", task.MediumPriority)

		q.Enqueue(task1)
		assert.Equal(t, 1, q.Size(), "Queue size should be 1 after enqueueing")

		dequeuedTask := q.Dequeue()
		assert.Equal(t, task1, dequeuedTask, "Dequeued task should match the enqueued task")
		assert.Equal(t, 0, q.Size(), "Queue size should be 0 after dequeueing")
	})

	t.Run("PriorityOrder", func(t *testing.T) {
		q := NewQueue()
		task1 := createTask("task1", task.LowPriority)
		task2 := createTask("task2", task.HighPriority)
		task3 := createTask("task3", task.MediumPriority)

		q.Enqueue(task1)
		q.Enqueue(task2)
		q.Enqueue(task3)

		assert.Equal(t, task2, q.Dequeue(), "High priority task should be dequeued first")
		assert.Equal(t, task3, q.Dequeue(), "Medium priority task should be dequeued second")
		assert.Equal(t, task1, q.Dequeue(), "Low priority task should be dequeued last")
	})

	t.Run("Clear", func(t *testing.T) {
		q := NewQueue()
		q.Enqueue(createTask("task1", task.LowPriority))
		q.Enqueue(createTask("task2", task.MediumPriority))

		assert.Equal(t, 2, q.Size(), "Queue size should be 2 before clearing")
		q.Clear()
		assert.Equal(t, 0, q.Size(), "Queue size should be 0 after clearing")
	})

	t.Run("ConcurrentOperations", func(t *testing.T) {
		q := NewQueue()
		taskCount := 1000
		var wg sync.WaitGroup
		wg.Add(taskCount * 2) // For both enqueueing and dequeueing

		// Concurrent Enqueue
		go func() {
			for i := 0; i < taskCount; i++ {
				go func(id int) {
					defer wg.Done()
					task := createTask(fmt.Sprintf("task%d", id), task.Priority(id%3))
					q.Enqueue(task)
				}(i)
			}
		}()

		// Concurrent Dequeue
		go func() {
			for i := 0; i < taskCount; i++ {
				go func() {
					defer wg.Done()
					_ = q.Dequeue()
				}()
			}
		}()

		// Use a timeout to prevent the test from hanging
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Test completed successfully
		case <-time.After(10 * time.Second): // Increased timeout
			t.Fatal("Test timed out")
		}

		assert.Equal(t, int64(taskCount), q.EnqueuedCount(), "Enqueued count should match the number of tasks")
		assert.Equal(t, int64(taskCount), q.DequeuedCount(), "Dequeued count should match the number of tasks")
		assert.Equal(t, 0, q.Size(), "Queue should be empty after all operations")
	})

	t.Run("BlockingDequeue", func(t *testing.T) {
		q := NewQueue()
		dequeued := make(chan interface{})

		go func() {
			dequeued <- q.Dequeue()
		}()

		time.Sleep(100 * time.Millisecond)
		task1 := createTask("task1", task.MediumPriority)
		q.Enqueue(task1)

		select {
		case dequeuedTask := <-dequeued:
			assert.Equal(t, task1, dequeuedTask, "Dequeued task should match the enqueued task")
		case <-time.After(1 * time.Second):
			t.Fatal("Dequeue did not unblock within 1 second")
		}
	})

	t.Run("DequeueChan", func(t *testing.T) {
		q := NewQueue()
		task1 := createTask("task1", task.MediumPriority)
		q.Enqueue(task1)

		select {
		case dequeuedTask := <-q.DequeueChan():
			assert.Equal(t, task1, dequeuedTask, "Dequeued task from channel should match the enqueued task")
		case <-time.After(1 * time.Second):
			t.Fatal("Failed to receive task from DequeueChan within 1 second")
		}
	})

	t.Run("Close", func(t *testing.T) {
		q := NewQueue()
		q.Enqueue(createTask("task1", task.MediumPriority))
		q.Close()

		// Check if the channel is closed
		_, ok := <-q.DequeueChan()
		assert.False(t, ok, "Channel should be closed after calling Close()")

		// Check if Enqueue panics after closing
		assert.Panics(t, func() { q.Enqueue(createTask("task2", task.MediumPriority)) }, "Enqueue should panic after queue is closed")
	})
}

func createTask(name string, priority task.Priority) *task.Task {
	t := task.NewTask(name, func() error { return nil }, "", 1, task.OneTimeType, time.Minute*1)
	t.Priority = priority
	return t
}
