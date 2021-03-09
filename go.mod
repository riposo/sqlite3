module github.com/riposo/sqlite3

go 1.16

replace github.com/riposo/riposo => ../riposo

require (
	github.com/bsm/minisql v0.1.0
	github.com/mattn/go-sqlite3 v1.14.6
	github.com/onsi/ginkgo v1.14.2
	github.com/onsi/gomega v1.10.4
	github.com/riposo/riposo v0.0.0-20210226155134-b4e129732a1c
	go.uber.org/multierr v1.6.0
)
