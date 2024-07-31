package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nex-gen-tech/jobsy"
)

func main() {
	// Create a new worker with mock DB adapter
	w := jobsy.NewWorker(&jobsy.WorkerOptions{})

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the worker
	go func() {
		if err := w.Start(ctx); err != nil {
			log.Printf("Worker stopped with error: %v", err)
		}
	}()

	// 1. Periodic Task (every 5 seconds)
	periodicTask, err := w.Schedule("Periodic Task", func() error {
		fmt.Println("Executing Periodic Task")
		return nil
	}, "0/5 * * * * ?")
	if err != nil {
		log.Fatalf("Failed to add Periodic Task: %v", err)
	}
	fmt.Printf("Added Periodic Task with ID: %s\n", periodicTask.ID)

	// 2. Immediate Task
	immediateTask, err := w.Schedule("Immediate Task", func() error {
		time.Sleep(2 * time.Second)
		fmt.Println("Executing Immediate Task")
		return nil
	}, "", jobsy.TaskOptions{
		MaxRetry: 3,
		TimeOut:  5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to add Immediate Task: %v", err)
	}
	fmt.Printf("Added Immediate Task with ID: %s\n", immediateTask.ID)

	// // 3. Failing Task with Retries
	// failingTask, err := w.AddTask("Failing Task", func() error {
	// 	fmt.Println("Executing Failing Task")
	// 	return fmt.Errorf("task failed")
	// }, "*/10 * * * * *", 3)
	// if err != nil {
	// 	log.Fatalf("Failed to add Failing Task: %v", err)
	// }
	// fmt.Printf("Added Failing Task with ID: %s\n", failingTask.ID)

	// 4. One-time Scheduled Task
	scheduledTask, err := w.RunAt("One-time Task", func() error {
		time.Sleep(2 * time.Second)
		fmt.Println("Executing One-time Task")
		return nil
	}, time.Now().Add(15*time.Second), jobsy.TaskOptions{
		MaxRetry: 3,
		TimeOut:  5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to schedule One-time Task: %v", err)
	}
	fmt.Printf("Scheduled One-time Task with ID: %s\n", scheduledTask.ID)

	scheduledTaskNew, err := w.RunAt("One-time Task New", func() error {
		time.Sleep(2 * time.Second)
		fmt.Println("Executing One-time Task New")
		return nil
	}, time.Now().Add(25*time.Second), jobsy.TaskOptions{
		MaxRetry: 3,
		TimeOut:  5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to schedule scheduledTaskNew One-time Task: %v", err)
	}
	fmt.Printf("scheduledTaskNew One-time Task with ID: %s\n", scheduledTaskNew.ID)

	// 5. Interval Task (every minute)
	intervalTask, err := w.RunEvery("Interval Task", func() error {
		time.Sleep(2 * time.Second)
		fmt.Println("Executing Interval Task")
		return nil
	}, 20*time.Second, jobsy.TaskOptions{
		MaxRetry: 3,
		TimeOut:  5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to add Interval Task: %v", err)
	}
	fmt.Printf("Added Interval Task with ID: %s\n", intervalTask.ID)

	// // 6. High-priority Task
	// highPriorityTask, err := w.AddTask("High Priority Task", func() error {
	// 	fmt.Println("Executing High Priority Task")
	// 	return nil
	// }, "*/30 * * * * *", 3)
	// if err != nil {
	// 	log.Fatalf("Failed to add High Priority Task: %v", err)
	// }
	// highPriorityTask.Priority = task.HighPriority
	// if err := dbAdapter.SaveTask(highPriorityTask); err != nil {
	// 	log.Fatalf("Failed to update High Priority Task: %v", err)
	// }
	// fmt.Printf("Added High Priority Task with ID: %s\n", highPriorityTask.ID)

	// // 7. Task with Dependency
	// dependentTask, err := w.AddTask("Dependent Task", func() error {
	// 	fmt.Println("Executing Dependent Task")
	// 	return nil
	// }, "", 3)
	// if err != nil {
	// 	log.Fatalf("Failed to add Dependent Task: %v", err)
	// }
	// dependentTask.Dependencies = []uuid.UUID{immediateTask.ID}
	// if err := dbAdapter.SaveTask(dependentTask); err != nil {
	// 	log.Fatalf("Failed to update Dependent Task: %v", err)
	// }
	// fmt.Printf("Added Dependent Task with ID: %s\n", dependentTask.ID)

	// // 8. Task with Timeout
	// timeoutTask, err := w.AddTask("Timeout Task", func() error {
	// 	time.Sleep(3 * time.Second)
	// 	fmt.Println("Executing Timeout Task")
	// 	return nil
	// }, "*/15 * * * * *", 3)
	// if err != nil {
	// 	log.Fatalf("Failed to add Timeout Task: %v", err)
	// }
	// timeoutTask.Timeout = 2 * time.Second
	// if err := dbAdapter.SaveTask(timeoutTask); err != nil {
	// 	log.Fatalf("Failed to update Timeout Task: %v", err)
	// }
	// fmt.Printf("Added Timeout Task with ID: %s\n", timeoutTask.ID)

	// // 9. Long-running Task
	// longRunningTask, err := w.AddTask("Long-running Task", func() error {
	// 	fmt.Println("Starting Long-running Task")
	// 	time.Sleep(30 * time.Second)
	// 	fmt.Println("Finished Long-running Task")
	// 	return nil
	// }, "", 1)
	// if err != nil {
	// 	log.Fatalf("Failed to add Long-running Task: %v", err)
	// }
	// fmt.Printf("Added Long-running Task with ID: %s\n", longRunningTask.ID)

	// // 10. Task with Progress Reporting
	// progressTask, err := w.AddTask("Progress Reporting Task", func() error {
	// 	total := 10
	// 	for i := 1; i <= total; i++ {
	// 		fmt.Printf("Progress: %d%%\n", i*10)
	// 		time.Sleep(1 * time.Second)
	// 	}
	// 	return nil
	// }, "", 1)
	// if err != nil {
	// 	log.Fatalf("Failed to add Progress Reporting Task: %v", err)
	// }
	// fmt.Printf("Added Progress Reporting Task with ID: %s\n", progressTask.ID)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Run the worker for a while
	select {
	case <-sigChan:
		fmt.Println("Received shutdown signal. Stopping worker...")
		cancel()
	case <-time.After(2 * time.Minute):
		fmt.Println("2 minutes elapsed. Stopping worker...")
		cancel()
	}

	// Wait for the worker to stop
	time.Sleep(2 * time.Second)

	// Print final task statuses
	tasks, _ := w.GetAllTasks()
	for _, t := range tasks {
		status, _ := w.GetTaskStatus(t.ID)
		fmt.Printf("Task %s (%s) final status: %s\n", t.Name, t.ID, status)
	}

	fmt.Println("Worker stopped. Exiting...")
}
