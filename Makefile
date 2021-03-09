GOTAGS='linux icu json1'

default: test

test:
	go test -tags $(GOTAGS) ./...

staticcheck:
	staticcheck ./...

build: plugin.so

plugin.so: go.mod go.sum $(shell find . ../riposo/ -name '*.go')
	go build -tags $(GOTAGS) -ldflags '-s -w' -buildmode=plugin -o $@ .
