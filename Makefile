BINARY_NAME=hello

build: clean
	go build -o bin/test -v src/test.go
	go build -o bin/server -v src/server.go
	go build -o bin/client -v src/client.go

clean:
	rm -rf bin/*
	rm -rf data/example/*

run_test:
	go build -o bin/test src/test.go
	bin/test

debug_s:
	go build -o bin/client -v src/client.go
	go build -o bin/server -gcflags "all=-N -l" src/server.go
	dlv --listen=localhost:54402 --headless=true --api-version=2 --backend=default exec bin/server

