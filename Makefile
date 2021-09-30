BINARY_NAME=hello

build: clean
	go build -o bin/test -v src/test.go
	go build -o bin/server -v src/server.go
	go build -o bin/client -v src/client.go


clean:
	rm -rf bin/*
	rm -rf data/example/*
