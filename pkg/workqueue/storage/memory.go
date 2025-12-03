package storage

import (
	"sync"

	"github.com/opencloud-eu/reva/v2/pkg/workqueue/task"
)

type Memory struct {
	tasks []*task.Task

	mu   sync.Mutex
	cond *sync.Cond
}

func NewMemory() *Memory {
	m := &Memory{}
	m.cond = sync.NewCond(&m.mu)
	return m
}

func (m *Memory) Queue(t *task.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tasks = append(m.tasks, t)
	m.cond.Signal()
	return nil
}

// Pull retrieves and removes the next task from the queue.
// it blocks until a new task is available
func (m *Memory) Pull() (*task.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.tasks) == 0 {
		m.cond.Wait()
	}

	task := m.tasks[0]
	m.tasks = m.tasks[1:]
	return task, nil
}

func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.tasks)
}
