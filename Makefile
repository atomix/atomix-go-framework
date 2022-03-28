# SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
#
# SPDX-License-Identifier: Apache-2.0

export CGO_ENABLED=0
export GO111MODULE=on

PARENT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../)

.PHONY: build

all: build

build: # @HELP build the source code
build:
	go build -v ./...

test: # @HELP run the unit tests and source code validation
test: build license # linters
	go test github.com/atomix/atomix-go-framework/pkg/...

protoc-gen-atomix: # @HELP build the source code
protoc-gen-atomix:
	GOOS=linux GOARCH=amd64 go build -o build/_output/protoc-gen-atomix ./cmd/protoc-gen-atomix

codegen: # @HELP build codegen Docker image
codegen: protoc-gen-atomix
	docker build . -f build/codegen/Dockerfile -t atomix/go-codegen:latest

primitives: codegen # @HELP compile the protobuf files (using protoc-go Docker)
	docker run -it \
		-v $(PARENT_DIR)/atomix-api:/go/src/github.com/atomix/atomix-api \
		-v `pwd`:/go/src/github.com/atomix/atomix-go-framework \
		-w /go/src/github.com/atomix/atomix-go-framework \
		--entrypoint build/bin/generate-primitives.sh \
		atomix/go-codegen:latest

coverage: # @HELP generate unit test coverage data
coverage: build linters license_check
	./build/bin/coveralls-coverage

linters: # @HELP examines Go source code and reports coding problems
	golangci-lint run

protos:
	docker run -it \
		-v $(PARENT_DIR)/atomix-api:/go/src/github.com/atomix/atomix-api \
	 	-v `pwd`:/go/src/github.com/atomix/atomix-go-framework \
		-w /go/src/github.com/atomix/atomix-go-framework \
		--entrypoint build/bin/compile_protos.sh \
		onosproject/protoc-go:stable

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
