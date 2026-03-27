package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// State represents the current state of a worker.
type State string

const (
	StateIdle    State = "idle"
	StateRunning State = "running"
)

// Status is a point-in-time snapshot of a worker's state.
type Status struct {
	ID    int    `json:"id"`
	State State  `json:"state"`
	Task  string `json:"task,omitempty"`
	Rows  int64  `json:"rows,omitempty"`
}

// worker tracks the mutable state of a single worker in the pool.
type worker struct {
	id    int
	state State
	task  string
	rows  atomic.Int64
	mu    sync.Mutex
}

func (w *worker) setRunning(task string) {
	w.mu.Lock()
	w.state = StateRunning
	w.task = task
	w.rows.Store(0)
	w.mu.Unlock()
}

func (w *worker) setIdle() {
	w.mu.Lock()
	w.state = StateIdle
	w.task = ""
	w.rows.Store(0)
	w.mu.Unlock()
}

func (w *worker) status() Status {
	w.mu.Lock()
	defer w.mu.Unlock()
	return Status{
		ID:    w.id,
		State: w.state,
		Task:  w.task,
		Rows:  w.rows.Load(),
	}
}

// Progress is passed to task functions so they can report row-level progress
// back to the worker pool.
type Progress struct {
	rows *atomic.Int64
}

// Add increments the row counter by delta.
func (p *Progress) Add(delta int64) {
	p.rows.Add(delta)
}

// Task is a unit of work submitted to the pool.
type Task struct {
	// Name identifies this task (e.g. table name). Shown in worker status.
	Name string
	// Fn is the work to execute. The Progress handle allows reporting
	// row-level progress. Return nil on success or an error to signal failure.
	Fn func(ctx context.Context, progress *Progress) error
}

// Result holds the outcome of a completed task.
type Result struct {
	Task string
	Rows int64
	Err  error
}

// Pool is a fixed-size worker pool that executes tasks concurrently.
type Pool struct {
	workers []*worker
}

// NewPool creates a pool with the given concurrency.
func NewPool(concurrency int) *Pool {
	workers := make([]*worker, concurrency)
	for i := range workers {
		workers[i] = &worker{id: i, state: StateIdle}
	}
	return &Pool{workers: workers}
}

// Workers returns a snapshot of all worker statuses.
func (p *Pool) Workers() []Status {
	statuses := make([]Status, len(p.workers))
	for i, w := range p.workers {
		statuses[i] = w.status()
	}
	return statuses
}

// Run executes all tasks using the pool's workers. It blocks until every task
// completes or the context is cancelled. If any task fails, the context is
// cancelled and remaining/in-flight tasks are aborted.
func (p *Pool) Run(ctx context.Context, tasks []Task) ([]Result, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	taskCh := make(chan Task, len(tasks))
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)

	resultCh := make(chan Result, len(tasks))
	var wg sync.WaitGroup

	for _, w := range p.workers {
		wg.Add(1)
		go func(w *worker) {
			defer wg.Done()
			for task := range taskCh {
				w.setRunning(task.Name)
				progress := &Progress{rows: &w.rows}

				err := task.Fn(ctx, progress)

				rows := w.rows.Load()
				w.setIdle()

				if err != nil {
					cancel()
					resultCh <- Result{Task: task.Name, Rows: rows, Err: fmt.Errorf("%s: %w", task.Name, err)}
					return
				}

				resultCh <- Result{Task: task.Name, Rows: rows}
			}
		}(w)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var results []Result
	var firstErr error
	for r := range resultCh {
		results = append(results, r)
		if r.Err != nil && firstErr == nil {
			firstErr = r.Err
		}
	}

	return results, firstErr
}
