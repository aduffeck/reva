package workqueue_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/opencloud-eu/reva/v2/pkg/workqueue"
	"github.com/opencloud-eu/reva/v2/pkg/workqueue/storage"
	"github.com/opencloud-eu/reva/v2/pkg/workqueue/task"
)

var _ = Describe("Workqueue", func() {
	var (
		memStorage workqueue.Storage
		wq         *workqueue.WorkQueue
	)

	BeforeEach(func() {
		memStorage = storage.NewMemory()
		wq = workqueue.New(workqueue.WithStorage(memStorage))
	})

	Describe("New", func() {
		It("creates a new workqueue", func() {
			wq := workqueue.New()
			Expect(wq).NotTo(BeNil())
		})

		It("takes a storage backend", func() {
			wq := workqueue.New(workqueue.WithStorage(memStorage))
			Expect(wq).NotTo(BeNil())

			Expect(wq.Storage()).To(Equal(memStorage))
		})
	})

	Describe("Push", func() {
		It("pushes a task to the queue", func() {
			task := task.NewTask("test", "test payload")
			Expect(wq.Push(task)).To(Succeed())

			Expect(wq.Storage().Len()).To(Equal(1))
		})
	})

	Describe("with a worker", func() {
		var (
			worker *workqueue.Worker
			result []string
		)

		BeforeEach(func() {
			worker = workqueue.NewWorker(wq)
			worker.Handle("test.process", func(t *task.Task) error {
				result = append(result, t.Payload())
				return nil
			})
		})

		AfterEach(func() {
			worker.Stop()
		})

		It("processes a task", func() {
			task := task.NewTask("test.process", "test payload")
			Expect(wq.Push(task)).To(Succeed())

			go worker.Start()

			Eventually(func(g Gomega) {
				g.Expect(wq.Storage().Len()).To(Equal(0))
				g.Expect(len(result)).To(Equal(1))
				g.Expect(result[0]).To(Equal("test payload"))
			}).Should(Succeed())
		})

		It("handles unknown task types", func() {
			task := task.NewTask("unknown.type", "test payload")
			Expect(wq.Push(task)).To(Succeed())

			go worker.Start()

			Eventually(func(g Gomega) {
				g.Expect(wq.Storage().Len()).To(Equal(0))
			}).Should(Succeed())
		})
	})
})
