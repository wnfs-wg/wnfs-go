default: build

build:
	go build -o wnfs ./cmd

install:
	go build -o wnfs ./cmd
	cp wnfs /usr/local/bin/wnfs

bench:
	go test -bench=. -run XXX -benchmem -benchtime 5s