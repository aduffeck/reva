package workqueue_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestWorkqueue(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Workqueue Suite")
}
