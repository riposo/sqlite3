GOTAGS='linux icu json1'

default: test

test:
	go test -tags $(GOTAGS) ./...

staticcheck:
	staticcheck ./...
