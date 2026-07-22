.PHONY: fmt vet test build

fmt:
	gofmt -w .

vet:
	go vet ./...

test:
	go test ./... -race -v

build:
	go build ./...
