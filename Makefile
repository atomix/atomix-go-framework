export CGO_ENABLED=0
export GO111MODULE=on

PARENT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../)

.PHONY: build

ifdef VERSION
BROKER_VERSION := $(VERSION)
else
BROKER_VERSION := latest
endif

all: build

build: # @HELP build the source code
build:
	go build -v ./...
	go build -o build/bin/atomix-broker ./cmd/atomix-broker

test: # @HELP run the unit tests and source code validation
test: build license_check linters
	go test github.com/atomix/atomix-go-sdk/pkg/...

images:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -gcflags=-trimpath=${GOPATH} -asmflags=-trimpath=${GOPATH} -o build/docker/atomix-broker/bin/atomix-broker ./cmd/atomix-broker
	docker build ./build/docker/atomix-broker -f build/docker/atomix-broker/Dockerfile -t atomix/atomix-broker:${BROKER_VERSION}

kind: images
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/atomix-broker:${BROKER_VERSION}

push: # @HELP push broker Docker image
	docker push atomix/atomix-broker:${BROKER_VERSION}

protoc-gen-atomix: # @HELP build the source code
protoc-gen-atomix:
	GOOS=linux GOARCH=amd64 go build -o build/_output/protoc-gen-atomix ./cmd/protoc-gen-atomix

codegen: # @HELP build codegen Docker image
codegen: protoc-gen-atomix
	docker build . -f build/codegen/Dockerfile -t atomix/go-codegen:latest

primitives: codegen # @HELP compile the protobuf files (using protoc-go Docker)
	docker run -it \
		-v $(PARENT_DIR)/atomix-api:/go/src/github.com/atomix/atomix-api \
		-v `pwd`:/go/src/github.com/atomix/atomix-go-sdk \
		-w /go/src/github.com/atomix/atomix-go-sdk \
		--entrypoint build/bin/generate-primitives.sh \
		atomix/go-codegen:latest

coverage: # @HELP generate unit test coverage data
coverage: build linters license_check
	./build/bin/coveralls-coverage

linters: # @HELP examines Go source code and reports coding problems
	golangci-lint run

license_check: # @HELP examine and ensure license headers exist
	./build/bin/license-check

protos:
	docker run -it \
		-v $(PARENT_DIR)/atomix-api:/go/src/github.com/atomix/atomix-api \
	 	-v `pwd`:/go/src/github.com/atomix/atomix-go-sdk \
		-w /go/src/github.com/atomix/atomix-go-sdk \
		--entrypoint build/bin/compile_protos.sh \
		onosproject/protoc-go:stable
