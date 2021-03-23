#!/bin/sh

proto_imports="./pkg:/go/src/github.com/atomix/api/proto:${GOPATH}/src/github.com/gogo/protobuf:${GOPATH}/src/github.com/gogo/protobuf/protobuf:${GOPATH}/src"

protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/protocol/rsm,plugins=grpc:pkg pkg/atomix/protocol/rsm/*.proto
protoc -I=$proto_imports --go_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/descriptor.proto=github.com/golang/protobuf/protoc-gen-go/descriptor,Matomix/primitive/meta/object.proto=github.com/atomix/api/go/atomix/primitive/meta,Matomix/primitive/meta/timestamp.proto=github.com/atomix/api/go/atomix/primitive/meta,import_path=atomix/protocol/gossip/primitive,plugins=grpc:pkg pkg/atomix/protocol/gossip/primitive/extensions.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Matomix/primitive/meta/object.proto=github.com/atomix/api/go/atomix/primitive/meta,Matomix/primitive/meta/timestamp.proto=github.com/atomix/api/go/atomix/primitive/meta,import_path=atomix/protocol/gossip,plugins=grpc:pkg pkg/atomix/protocol/gossip/protocol.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Matomix/primitive/meta/object.proto=github.com/atomix/api/go/atomix/primitive/meta,Matomix/protocol/gossip/primitive/extensions.proto=github.com/atomix/go-framework/pkg/atomix/protocol/gossip/primitive,Matomix/primitive/meta/timestamp.proto=github.com/atomix/api/go/atomix/primitive/meta,import_path=atomix/protocol/gossip/counter,plugins=grpc:pkg pkg/atomix/protocol/gossip/counter/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Matomix/primitive/meta/object.proto=github.com/atomix/api/go/atomix/primitive/meta,Matomix/protocol/gossip/primitive/extensions.proto=github.com/atomix/go-framework/pkg/atomix/protocol/gossip/primitive,Matomix/primitive/meta/timestamp.proto=github.com/atomix/api/go/atomix/primitive/meta,import_path=atomix/protocol/gossip/map,plugins=grpc:pkg pkg/atomix/protocol/gossip/map/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Matomix/primitive/meta/object.proto=github.com/atomix/api/go/atomix/primitive/meta,Matomix/protocol/gossip/primitive/extensions.proto=github.com/atomix/go-framework/pkg/atomix/protocol/gossip/primitive,Matomix/primitive/meta/timestamp.proto=github.com/atomix/api/go/atomix/primitive/meta,import_path=atomix/protocol/gossip/set,plugins=grpc:pkg pkg/atomix/protocol/gossip/set/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Matomix/primitive/meta/object.proto=github.com/atomix/api/go/atomix/primitive/meta,Matomix/protocol/gossip/primitive/extensions.proto=github.com/atomix/go-framework/pkg/atomix/protocol/gossip/primitive,Matomix/primitive/meta/timestamp.proto=github.com/atomix/api/go/atomix/primitive/meta,import_path=atomix/protocol/gossip/value,plugins=grpc:pkg pkg/atomix/protocol/gossip/value/*.proto
