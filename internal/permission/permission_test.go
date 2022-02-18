package permission_test

import (
	"context"
	"os"
	"testing"

	"github.com/riposo/riposo/pkg/conn/permission"
	"github.com/riposo/riposo/pkg/conn/permission/testdata"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	. "github.com/riposo/sqlite3/internal/permission"
)

var _ = Describe("Backend", func() {
	var link testdata.LikeBackend

	BeforeEach(func() {
		link.Backend = instance
	})

	Describe("common", func() {
		testdata.BehavesLikeBackend(&link)
	})
})

// --------------------------------------------------------------------

var instance permission.Backend
var tempDir string

var _ = BeforeSuite(func() {
	var err error
	tempDir, err := os.MkdirTemp("", "riposo-plugins-sqlite3-test")
	Expect(err).NotTo(HaveOccurred())

	dsn := "sqlite3://" + tempDir + "/db.sqlite3"
	if val := os.Getenv("RIPOSO_DATABASE_DSN"); val != "" {
		dsn = val
	}

	instance, err = Connect(context.Background(), dsn)
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
	RunSpecs(t, "internal/permission")
}
