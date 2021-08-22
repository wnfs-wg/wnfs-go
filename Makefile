default: build

build:
	go build -o wnfs ./cmd

install:
	go build -o wnfs ./cmd
	cp wnfs /usr/local/bin/wnfs

bench:
	cd ipfs && go test -bench=. -run XXX -benchmem && echo ""
	go test -bench=BenchmarkPublic -run XXX -benchmem && echo ""
	go test -bench=BenchmarkPrivate -run XXX -benchmem