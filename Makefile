BINARY_NAME=hello

build: clean
	mkdir -p target/data/example target/data/meta
	go build -o target/bin/test -v src/test.go
	go build -o target/bin/server -v src/server.go
	go build -o target/bin/client -v src/client.go

clean:
	rm -rf target/bin/*
	rm -rf target/data/example/*

run_test:
	go build -o target/bin/test src/test.go
	target/bin/test

debug_s:
	go build -o target/bin/client -v src/client.go
	go build -o target/bin/server -gcflags "all=-N -l" src/server.go
	dlv --listen=localhost:54402 --headless=true --api-version=2 --backend=default exec target/bin/server

