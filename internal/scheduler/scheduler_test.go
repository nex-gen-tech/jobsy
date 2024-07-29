package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewScheduler(t *testing.T) {
	s := NewScheduler()
	assert.NotNil(t, s.cron)
	assert.NotNil(t, s.jobs)
}

func TestAddTask(t *testing.T) {
	s := NewScheduler()
	id := uuid.New()
	executed := make(chan bool, 1)

	err := s.AddTask(id, "* * * * * *", func() {
		executed <- true
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go s.Start(ctx)

	select {
	case <-executed:
		// Task executed successfully
	case <-ctx.Done():
		t.Fatal("Task was not executed within the expected time")
	}
}

func TestRemoveTask(t *testing.T) {
	s := NewScheduler()
	id := uuid.New()
	executed := make(chan bool, 1)

	err := s.AddTask(id, "* * * * * *", func() {
		executed <- true
	})
	assert.NoError(t, err)

	s.RemoveTask(id)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go s.Start(ctx)

	select {
	case <-executed:
		t.Fatal("Removed task was executed")
	case <-ctx.Done():
		// Expected behavior: context deadline exceeded
	}
}

func TestScheduleOnce(t *testing.T) {
	s := NewScheduler()
	id := uuid.New()
	executed := make(chan bool, 1)

	s.ScheduleOnce(id, time.Now().Add(500*time.Millisecond), func() {
		executed <- true
	})

	select {
	case <-executed:
		// Task executed successfully
	case <-time.After(1 * time.Second):
		t.Fatal("One-time task was not executed within the expected time")
	}
}

func TestAddTaskWithInterval(t *testing.T) {
	s := NewScheduler()
	id := uuid.New()
	executed := make(chan bool, 3)

	err := s.AddTaskWithInterval(id, 200*time.Millisecond, func() {
		executed <- true
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	go s.Start(ctx)

	count := 0
	for {
		select {
		case <-executed:
			count++
			if count == 2 {
				return
			}
		case <-ctx.Done():
			t.Fatalf("Interval task was executed %d times, expected 2", count)
		}
	}
}

func TestConcurrency(t *testing.T) {
	s := NewScheduler()
	var wg sync.WaitGroup
	taskCount := 100

	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := uuid.New()
			err := s.AddTask(id, "* * * * * *", func() {})
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	assert.Equal(t, taskCount, len(s.jobs))
}

func TestStartAndStop(t *testing.T) {
	s := NewScheduler()
	id := uuid.New()
	executed := make(chan bool, 1)

	err := s.AddTask(id, "* * * * * *", func() {
		executed <- true
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Start(ctx)
	}()

	// Wait for task to execute
	<-executed

	// Stop the scheduler
	cancel()
	wg.Wait()

	// Ensure no more executions happen
	select {
	case <-executed:
		t.Fatal("Task executed after scheduler was stopped")
	case <-time.After(1500 * time.Millisecond):
		// Expected behavior: no execution after stop
	}
}

func TestScheduleOnceOverwrite(t *testing.T) {
	s := NewScheduler()
	id := uuid.New()
	executed := make(chan int, 2)

	s.ScheduleOnce(id, time.Now().Add(500*time.Millisecond), func() {
		executed <- 1
	})

	time.Sleep(100 * time.Millisecond)

	// Attempt to schedule the same task again
	s.ScheduleOnce(id, time.Now().Add(200*time.Millisecond), func() {
		executed <- 2
	})

	select {
	case result := <-executed:
		assert.Equal(t, 1, result, "The first scheduled task should have executed")
	case <-time.After(1 * time.Second):
		t.Fatal("No task was executed within the expected time")
	}

	select {
	case <-executed:
		t.Fatal("Both tasks were executed, but only one should have been")
	case <-time.After(500 * time.Millisecond):
		// Expected behavior: no second execution
	}
}
