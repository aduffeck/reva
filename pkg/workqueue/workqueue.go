package workqueue

import (
	"github.com/opencloud-eu/reva/v2/pkg/workqueue/task"
)

type Storage interface {
	Pull() (*task.Task, error)
}

type Worker struct {
}

type WorkQueue struct {
	storage Storage
}

func New(opts ...Option) *WorkQueue {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	return &WorkQueue{
		storage: o.storage,
	}
}

func (wq *WorkQueue) Storage() Storage {
	return wq.storage
}
