default: build

build:
	cd ./cmd && go build -o wnfs .

install:
	cd ./cmd && go build -o wnfs .
	cp wnfs /usr/local/bin/wnfs

test:
	go test --race --coverprofile=coverage.txt ./...

coverage:
	go tool cover --html=coverage.txt

bench:
	cd cmd/ipfs && go test -bench=. -run XXX -benchmem && echo ""
	go test -bench=BenchmarkPublic -run XXX -benchmem && echo ""
	go test -bench=BenchmarkPrivate -run XXX -benchmem