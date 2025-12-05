package task

import "github.com/google/uuid"

// Task represents a unit of work in the queue.
type Task struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Status  string `json:"status"`
	Payload string `json:"payload"`
}

// NewTask creates a new task with the given type and payload.
func NewTask(taskType, payload string) *Task {
	return &Task{
		ID:      uuid.New().String(),
		Type:    taskType,
		Payload: payload,
		Status:  "pending",
	}
}

// SetStatus sets the current status of the task.
func (t *Task) SetStatus(status string) {
	t.Status = status
}
