package storage

import (
	"sync"

	"github.com/opencloud-eu/reva/v2/pkg/workqueue/task"
)

// Memory is an in-memory implementation of the Storage interface for the workqueue.
type Memory struct {
	pending    []*task.Task
	processing map[string]*task.Task

	mu   sync.Mutex
	cond *sync.Cond
}

// NewMemory creates a new in-memory storage backend for the workqueue.
func NewMemory() *Memory {
	m := &Memory{
		pending:    []*task.Task{},
		processing: make(map[string]*task.Task),
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

// Queue adds a task to the queue.
func (m *Memory) Queue(t *task.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.pending = append(m.pending, t)
	m.cond.Signal()
	return nil
}

// Pull retrieves and removes the next task from the queue.
// If the queue is empty, it blocks until a task is available.
func (m *Memory) Pull() (*task.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.pending) == 0 {
		m.cond.Wait()
	}

	task := m.pending[0]
	m.pending = m.pending[1:]

	task.SetStatus("processing")
	m.processing[task.ID] = task

	return task, nil
}

// Len returns the number of tasks currently in the queue.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.pending) + len(m.processing)
}

// Ack acknowledges the successful processing of a task.
func (m *Memory) Ack(t *task.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.processing, t.ID)
	return nil
}

// Nack indicates that the processing of a task has failed.
func (m *Memory) Nack(t *task.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.processing[t.ID]; exists {
		delete(m.processing, t.ID)
		t.SetStatus("nack")
		m.pending = append(m.pending, t)
		m.cond.Signal()
	}
	return nil
}
