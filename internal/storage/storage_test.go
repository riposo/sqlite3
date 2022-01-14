package storage_test

import (
	"context"
	"os"
	"testing"

	"github.com/riposo/riposo/pkg/conn/storage"
	"github.com/riposo/riposo/pkg/conn/storage/testdata"
	"github.com/riposo/riposo/pkg/mock"
	"github.com/riposo/riposo/pkg/params"
	"github.com/riposo/riposo/pkg/riposo"

	. "github.com/bsm/ginkgo"
	. "github.com/bsm/gomega"
	. "github.com/riposo/sqlite3/internal/storage"
)

var _ = Describe("Backend", func() {
	var link testdata.LikeBackend

	BeforeEach(func() {
		instance.(reloadable).ReloadHelpers(mock.Helpers())
		link.Backend = instance
		link.SkipFilters = []params.Operator{
			params.OperatorContains,
			params.OperatorContainsAny,
		}
	})

	testdata.BehavesLikeBackend(&link)
})

// --------------------------------------------------------------------

type reloadable interface {
	ReloadHelpers(riposo.Helpers)
}

var instance storage.Backend
var tempDir string

var _ = BeforeSuite(func() {
	var err error
	tempDir, err := os.MkdirTemp("", "riposo-plugins-sqlite3-test")
	Expect(err).NotTo(HaveOccurred())

	dsn := "sqlite3://" + tempDir + "/db.sqlite3"
	if val := os.Getenv("RIPOSO_DATABASE_DSN"); val != "" {
		dsn = val
	}

	instance, err = Connect(context.Background(), dsn, mock.Helpers())
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if instance != nil {
		Expect(instance.Close()).To(Succeed())
	}
	if tempDir != "" {
		Expect(os.RemoveAll(tempDir)).To(Succeed())
	}
})

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "internal/storage")
}
