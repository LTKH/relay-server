# Relay-server

This project adds a basic high availability layer to InfluxDB. With the right architecture and disaster recovery processes, this achieves a highly available setup.

*NOTE:* `relay-server` must be built with Go 1.5+

## Usage

To build from source and run:

```sh
$ # Download relay-server
$ git clone https://github.com/LTKH/relay-server.git
$ cd relay-server
$ # Edit your configuration file
$ vi config/default.toml
$ # Build and start relay-server
$ $GOPATH=$(dirname "$(pwd)") go build -o bin/relay-server relay-server.go
$ bin/relay-server -config config/default.toml
```

## Configuration

...
