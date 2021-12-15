default: build

build:
	go build -o wnfs ./cmd

.PHONY: install
install: build
	@install -C wnfs /usr/local/go/bin/wnfs

test:
	go test --race --coverprofile=coverage.txt ./...

coverage:
	go tool cover --html=coverage.txt

bench:
	cd cmd/ipfs && go test -bench=. -run XXX -benchmem && echo ""
	go test -bench=BenchmarkPublic -run XXX -benchmem && echo ""
	go test -bench=BenchmarkPrivate -run XXX -benchmem