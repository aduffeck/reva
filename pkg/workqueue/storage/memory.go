package storage

import "github.com/opencloud-eu/reva/v2/pkg/workqueue/task"

type Memory struct {
}

func NewMemory() *Memory {
	return &Memory{}
}

func (m *Memory) Pull() (*task.Task, error) {
	return nil, nil
}
