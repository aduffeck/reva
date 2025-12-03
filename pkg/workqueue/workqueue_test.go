package workqueue_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	wq "github.com/opencloud-eu/reva/v2/pkg/workqueue"
	"github.com/opencloud-eu/reva/v2/pkg/workqueue/storage"
)

var _ = Describe("Workqueue", func() {
	var (
		memStorage = storage.NewMemory()
	)

	Describe("New", func() {
		It("should create a new workqueue", func() {
			wq := wq.New()
			Expect(wq).NotTo(BeNil())
		})

		It("takes a storage backend", func() {
			wq := wq.New(wq.WithStorage(memStorage))
			Expect(wq).NotTo(BeNil())

			Expect(wq.Storage()).To(Equal(memStorage))
		})
	})
})
