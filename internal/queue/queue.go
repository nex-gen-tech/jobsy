package queue

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nex-gen-tech/jobsy/internal/task"
)

// Queue represents a priority queue for tasks.
type Queue struct {
	tasks    priorityQueue
	mu       sync.Mutex
	cond     *sync.Cond
	taskChan chan interface{}
	closed   int32
	size     int64
	enqueued int64
	dequeued int64
}

// priorityQueue is a slice of tasks.
type priorityQueue []*task.Task

// Len returns the length of the priority queue.
func (pq priorityQueue) Len() int { return len(pq) }

// Less reports whether the task at index i should sort before the task at index j.
func (pq priorityQueue) Less(i, j int) bool {
	if pq[i].Priority == pq[j].Priority {
		return pq[i].CreatedAt.Before(pq[j].CreatedAt)
	}
	return pq[i].Priority > pq[j].Priority
}

// Swap swaps the tasks at indexes i and j.
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push adds a task to the priority queue.
func (pq *priorityQueue) Push(x interface{}) {
	item := x.(*task.Task)
	*pq = append(*pq, item)
}

// Pop removes and returns the highest priority task from the priority queue.
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// NewQueue creates a new instance of Queue.
func NewQueue() *Queue {
	q := &Queue{
		tasks:    make(priorityQueue, 0),
		taskChan: make(chan interface{}, 10000),
	}
	q.cond = sync.NewCond(&q.mu)
	go q.startTaskDispatcher()
	return q
}

func (q *Queue) Close() {
	if atomic.CompareAndSwapInt32(&q.closed, 0, 1) {
		q.mu.Lock()
		close(q.taskChan)
		q.cond.Broadcast()
		q.mu.Unlock()
	}
}

// startTaskDispatcher starts the task dispatcher goroutine that continuously
// dequeues tasks from the queue and sends them to the task channel for processing.
func (q *Queue) startTaskDispatcher() {
	for {
		q.mu.Lock()
		for q.tasks.Len() == 0 && atomic.LoadInt32(&q.closed) == 0 {
			q.cond.Wait()
		}
		if atomic.LoadInt32(&q.closed) == 1 {
			q.mu.Unlock()
			return
		}
		var task interface{}
		if q.tasks.Len() > 0 {
			task = heap.Pop(&q.tasks)
			atomic.AddInt64(&q.size, -1)
			atomic.AddInt64(&q.dequeued, 1)
		}
		q.mu.Unlock()

		if task != nil {
			select {
			case q.taskChan <- task:
			default:
				// Channel is full, put the task back
				q.mu.Lock()
				heap.Push(&q.tasks, task)
				atomic.AddInt64(&q.size, 1)
				atomic.AddInt64(&q.dequeued, -1)
				q.mu.Unlock()
			}
		}

		// Add a small sleep to prevent tight looping
		time.Sleep(1 * time.Millisecond)
	}
}

func (q *Queue) IsClosed() bool {
	return atomic.LoadInt32(&q.closed) == 1
}

// Enqueue adds a task to the queue.
func (q *Queue) Enqueue(t interface{}) {
	if atomic.LoadInt32(&q.closed) == 1 {
		panic("cannot enqueue to a closed queue")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	heap.Push(&q.tasks, t)
	atomic.AddInt64(&q.size, 1)
	atomic.AddInt64(&q.enqueued, 1)
	q.cond.Signal()
}

// Dequeue removes and returns the next task from the queue.
func (q *Queue) Dequeue() interface{} {
	t, ok := <-q.taskChan
	if !ok {
		return nil
	}
	return t
}

// DequeueChan returns a channel for dequeuing tasks from the queue.
func (q *Queue) DequeueChan() <-chan interface{} {
	return q.taskChan
}

// Size returns the current size of the queue.
func (q *Queue) Size() int {
	return int(atomic.LoadInt64(&q.size))
}

// EnqueuedCount returns the total number of tasks enqueued.
func (q *Queue) EnqueuedCount() int64 {
	return atomic.LoadInt64(&q.enqueued)
}

// DequeuedCount returns the total number of tasks dequeued.
func (q *Queue) DequeuedCount() int64 {
	return atomic.LoadInt64(&q.dequeued)
}

// Clear removes all tasks from the queue.
func (q *Queue) Clear() {
	q.mu.Lock()
	q.tasks = make(priorityQueue, 0)
	atomic.StoreInt64(&q.size, 0)
	atomic.StoreInt64(&q.enqueued, 0)
	atomic.StoreInt64(&q.dequeued, 0)
	q.mu.Unlock()
}
