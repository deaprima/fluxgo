.PHONY: fmt vet lint test cover bench bench-compare fuzz build

fmt:
	gofmt -w .

vet:
	go vet ./...

lint: vet
	go run honnef.co/go/tools/cmd/staticcheck@latest ./...

test:
	go test ./... -race -v

cover:
	go test ./... -coverprofile=coverage.out
	go tool cover -func=coverage.out

bench:
	go test ./... -run xxx -bench . -benchmem

bench-compare:
	cd bench && go test -run xxx -bench . -benchmem

fuzz:
	go test -run xxx -fuzz FuzzRollingPercentile -fuzztime 1m .

build:
	go build ./...
