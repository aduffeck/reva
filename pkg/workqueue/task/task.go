package task

type Task struct {
	taskType string
	payload  string
}

func NewTask(taskType, payload string) *Task {
	return &Task{
		taskType: taskType,
		payload:  payload,
	}
}

func (t *Task) Type() string {
	return t.taskType
}

func (t *Task) Payload() string {
	return t.payload
}
