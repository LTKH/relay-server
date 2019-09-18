GOPATH=`pwd`

build:
	@echo "Building Relay-server..."
	@GOPATH=${GOPATH} go build -o bin/relay-server relay-server.go

run:
	@echo "Run Relay-server..."
	@GOPATH=${GOPATH} go run relay-server.go -config conf/default.toml

test:
	@GOPATH=${GOPATH} go vet .
	@GOPATH=${GOPATH} golangci-lint run . src/relay-server/...

