package workqueue_test

import (
	"os"

	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/opencloud-eu/reva/v2/pkg/workqueue"
	"github.com/opencloud-eu/reva/v2/pkg/workqueue/storage/nats"
	"github.com/opencloud-eu/reva/v2/pkg/workqueue/task"
)

var _ = Describe("Workqueue with a nats storage", func() {
	var (
		natsStorage workqueue.Storage
		wq          *workqueue.WorkQueue
		natsServer  *server.Server
		tmpDir      string
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "nats-workqueue-test")
		Expect(err).ToNot(HaveOccurred())

		// Start an embedded NATS server on a random port
		opts := natstest.DefaultTestOptions
		opts.Port = -1
		opts.JetStream = true
		opts.StoreDir = tmpDir // Tell NATS to store files here
		natsServer = natstest.RunServer(&opts)

		// Pass the dynamic server URL to the storage
		natsStorage, err = nats.New(
			nats.WithStreamName("testqueue"),
			nats.WithNatsConfig(
				nats.NatsConfig{
					Endpoint: natsServer.ClientURL(),
				},
			))
		Expect(err).ToNot(HaveOccurred())

		wq = workqueue.New(workqueue.WithStorage(natsStorage))
	})

	AfterEach(func() {
		if natsServer != nil {
			natsServer.Shutdown()
		}
		// Clean up the temporary directory
		if tmpDir != "" {
			os.RemoveAll(tmpDir)
		}
	})

	Describe("New", func() {
		It("creates a new workqueue", func() {
			wq := workqueue.New()
			Expect(wq).NotTo(BeNil())
		})

		It("takes a storage backend", func() {
			wq := workqueue.New(workqueue.WithStorage(natsStorage))
			Expect(wq).NotTo(BeNil())

			Expect(wq.Storage()).To(Equal(natsStorage))
		})
	})

	Describe("Push", func() {
		It("pushes a task to the queue", func() {
			task := task.NewTask("test", "test payload")
			Expect(wq.Push(task)).To(Succeed())

			// We might need to wait a bit for NATS to propagate if it's async,
			// but for a unit test with local server, it's usually fast enough.
			// Using Eventually is safer.
			Eventually(func() int {
				return wq.Storage().Len()
			}).Should(Equal(1))
		})
	})

	Describe("with a worker", func() {
		var (
			worker *workqueue.Worker
			result []string
		)

		BeforeEach(func() {
			worker = wq.NewWorker()
			worker.Handle("test.process", func(t *task.Task) error {
				result = append(result, t.Payload)
				return nil
			})
		})

		AfterEach(func() {
			worker.Stop()
		})

		It("processes a task", func() {
			task := task.NewTask("test.process", "test payload")
			Expect(wq.Push(task)).To(Succeed())
			Eventually(func() int { return wq.Storage().Len() }).Should(Equal(1))

			go worker.Start()

			Eventually(func(g Gomega) {
				g.Expect(len(result)).To(Equal(1))
				g.Expect(wq.Storage().Len()).To(Equal(0))
				g.Expect(result[0]).To(Equal("test payload"))

			}).Should(Succeed())
		})

		It("nacks unknown task types", func() {
			task := task.NewTask("unknown.type", "test payload")
			Expect(wq.Push(task)).To(Succeed())
			Eventually(func() int { return wq.Storage().Len() }).Should(Equal(1))

			go worker.Start()

			Consistently(func(g Gomega) {
				g.Expect(wq.Storage().Len()).To(Equal(1))
			}).Should(Succeed())
		})
	})
})
