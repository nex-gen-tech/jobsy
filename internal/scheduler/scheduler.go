package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

var _ SchedulerInterface = (*Scheduler)(nil)

type SchedulerInterface interface {
	AddTask(id string, cronExpr string, task func()) error
	RemoveTask(id string)
	ScheduleOnce(id string, when time.Time, task func())
	Start(ctx context.Context)
	Stop()
	AddTaskWithInterval(id string, interval time.Duration, task func()) error
}

// Scheduler represents a job scheduler.
type Scheduler struct {
	cron     *cron.Cron
	jobs     map[string]cron.EntryID
	onceJobs sync.Map
	mu       sync.RWMutex
	stopCh   chan struct{}
	doneCh   chan struct{}
	logger   *log.Logger
}

// NewScheduler creates a new instance of Scheduler.
func NewScheduler() *Scheduler {
	return &Scheduler{
		cron:   cron.New(cron.WithSeconds()),
		jobs:   make(map[string]cron.EntryID),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
		logger: log.New(log.Writer(), "SCHEDULER: ", log.LstdFlags),
	}
}

// AddTask adds a task to the scheduler with the specified ID, cron expression, and task function.
// If a task with the same ID already exists, it does nothing and returns nil.
// Otherwise, it schedules the task to run at the specified cron expression and returns nil.
func (s *Scheduler) AddTask(id string, cronExpr string, task func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[id]; exists {
		return nil // Task already scheduled, do nothing
	}

	entryID, err := s.cron.AddFunc(cronExpr, task)
	if err != nil {
		return err
	}

	s.jobs[id] = entryID
	return nil
}

// RemoveTask removes a task from the scheduler with the specified ID.
func (s *Scheduler) RemoveTask(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entryID, exists := s.jobs[id]; exists {
		s.cron.Remove(entryID)
		delete(s.jobs, id)
	}

	if timer, loaded := s.onceJobs.LoadAndDelete(id); loaded {
		timer.(*time.Timer).Stop()
	}
}

// ScheduleOnce schedules a task to run once at the specified time.
// If a task with the same ID is already scheduled, it cancels the existing timer and schedules the new task.
func (s *Scheduler) ScheduleOnce(id string, when time.Time, task func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, loaded := s.onceJobs.LoadOrStore(id, struct{}{}); loaded {
		return
	}

	wrapper := func() {
		task()
		s.onceJobs.Delete(id)
	}

	duration := time.Until(when)
	if duration < 0 {
		go wrapper()
	} else {
		timer := time.AfterFunc(duration, wrapper)
		s.onceJobs.Store(id, timer)
	}
}

// Start starts the scheduler in a separate goroutine.
// It takes a context as a parameter to allow graceful shutdown.
func (s *Scheduler) Start(ctx context.Context) {
	s.cron.Start()
	go func() {
		defer close(s.doneCh)
		select {
		case <-ctx.Done():
		case <-s.stopCh:
		}
		s.cron.Stop()
	}()
}

// Stop stops the scheduler and waits for all tasks to complete.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cron != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stopChan := make(chan struct{})
		go func() {
			s.cron.Stop()
			close(stopChan)
		}()

		select {
		case <-stopChan:
			// Scheduler stopped normally
		case <-ctx.Done():
			// Timeout occurred, log the error but continue
			log.Println("Scheduler.Stop timed out after 5 seconds")
		}
	}
}

// AddTaskWithInterval adds a task with the specified interval to the scheduler.
// It takes an ID, interval duration, and a task function as parameters.
// If a task with the same ID already exists, it does nothing and returns nil.
// Otherwise, it schedules the task to run at the specified interval and returns nil.
func (s *Scheduler) AddTaskWithInterval(id string, interval time.Duration, task func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[id]; exists {
		return nil // Task already scheduled, do nothing
	}

	entryID := s.cron.Schedule(cron.Every(interval), cron.FuncJob(task))
	s.jobs[id] = entryID
	return nil
}
