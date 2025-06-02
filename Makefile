# Basic Go Makefile
BINARY_NAME=confluent-cloud-exporter
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "1.0.0-dev")
BUILD_TIME=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS=-ldflags "-X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}"

.PHONY: all build clean test lint run help

all: test build

build:
	@echo "Building ${BINARY_NAME}..."
	@go build ${LDFLAGS} -o ${BINARY_NAME} main.go

test:
	@echo "Running tests..."
	@go test -v ./...

lint:
	@echo "Linting code..."
	@golangci-lint run ./...

clean:
	@echo "Cleaning up..."
	@rm -f ${BINARY_NAME}

run: build
	@echo "Running ${BINARY_NAME}..."
	@./${BINARY_NAME} -config.file=config.yaml

help:
	@echo "Available commands:"
	@echo "  make build	- Build the binary"
	@echo "  make test	 - Run tests"
	@echo "  make lint	 - Run linter"
	@echo "  make clean	- Remove binary"
	@echo "  make run	  - Build and run the binary"