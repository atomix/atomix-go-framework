#!/bin/sh

proto_imports="./pkg:${GOPATH}/src/github.com/gogo/protobuf:${GOPATH}/src/github.com/gogo/protobuf/protobuf:${GOPATH}/src"

protoc -I=$proto_imports --gogofaster_out=import_path=atomix/counter,plugins=grpc:pkg pkg/atomix/counter/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/election,plugins=grpc:pkg pkg/atomix/election/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/indexedmap,plugins=grpc:pkg pkg/atomix/indexedmap/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/leader,plugins=grpc:pkg pkg/atomix/leader/*.proto
protoc -I=$proto_imports --gogofaster_out=import_path=atomix/list,plugins=grpc:pkg pkg/atomix/list/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/lock,plugins=grpc:pkg pkg/atomix/lock/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/map,plugins=grpc:pkg pkg/atomix/map/*.proto
protoc -I=$proto_imports --gogofaster_out=import_path=atomix/set,plugins=grpc:pkg pkg/atomix/set/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/node,plugins=grpc:pkg pkg/atomix/node/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/service,plugins=grpc:pkg pkg/atomix/service/*.proto
protoc -I=$proto_imports --gogofaster_out=import_path=atomix/value,plugins=grpc:pkg pkg/atomix/value/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/log,plugins=grpc:pkg pkg/atomix/log/*.proto