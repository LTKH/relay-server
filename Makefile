GOPATH=`pwd`

build:
	@echo "Building Relay-server..."
	@GOPATH=${GOPATH} go build -o bin/relay-server relay-server.go

run:
	@echo "Run Relay-server..."
	@bin/relay-server -config conf/default.toml
