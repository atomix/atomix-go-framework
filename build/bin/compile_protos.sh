#!/bin/sh

proto_imports="./pkg:/go/src/github.com/atomix/api/proto:${GOPATH}/src/github.com/gogo/protobuf:${GOPATH}/src/github.com/gogo/protobuf/protobuf:${GOPATH}/src"

protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/protocol/rsm,plugins=grpc:pkg pkg/atomix/protocol/rsm/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Matomix/primitive/meta/object.proto=github.com/atomix/api/go/atomix/primitive/meta,import_path=atomix/protocol/gossip,plugins=grpc:pkg pkg/atomix/protocol/gossip/*.proto
