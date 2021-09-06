GOTAGS='linux icu json1'

default: test lint

test:
	go test -tags $(GOTAGS) ./...

lint:
	golangci-lint run
