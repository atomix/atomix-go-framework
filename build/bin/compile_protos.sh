#!/bin/sh

proto_imports="./pkg:/go/src/github.com/atomix/atomix-api/proto:${GOPATH}/src/github.com/gogo/protobuf:${GOPATH}/src/github.com/gogo/protobuf/protobuf:${GOPATH}/src"

go_import_paths="Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types"
go_import_paths="${go_import_paths},Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types"
go_import_paths="${go_import_paths},Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types"
go_import_paths="${go_import_paths},Mgoogle/protobuf/descriptor.proto=github.com/golang/protobuf/protoc-gen-go/descriptor"
go_import_paths="${go_import_paths},Matomix/protocol/protocol.proto=github.com/atomix/atomix-api/go/atomix/protocol"
go_import_paths="${go_import_paths},Matomix/primitive/primitive.proto=github.com/atomix/atomix-api/go/atomix/primitive"
go_import_paths="${go_import_paths},Matomix/primitive/meta/object.proto=github.com/atomix/atomix-api/go/atomix/primitive/meta"
go_import_paths="${go_import_paths},Matomix/primitive/meta/timestamp.proto=github.com/atomix/atomix-api/go/atomix/primitive/meta"
go_import_paths="${go_import_paths},Matomix/primitive/timestamp/timestamp.proto=github.com/atomix/atomix-api/go/atomix/primitive/timestamp"

protoc -I=$proto_imports --go_out=$go_import_paths,import_path=atomix/storage/protocol/gossip/primitive,plugins=grpc:pkg pkg/atomix/storage/protocol/gossip/primitive/extensions.proto

protoc -I=$proto_imports --gogofaster_out=$go_import_paths,import_path=atomix/driver/proxy/gossip,plugins=grpc:pkg   pkg/atomix/driver/proxy/gossip/config.proto
protoc -I=$proto_imports --gogofaster_out=$go_import_paths,import_path=atomix/driver/proxy/rsm,plugins=grpc:pkg   pkg/atomix/driver/proxy/rsm/config.proto

protoc -I=$proto_imports --gogofaster_out=$go_import_paths,import_path=atomix/storage/protocol/rsm,plugins=grpc:pkg      pkg/atomix/storage/protocol/rsm/*.proto
protoc -I=$proto_imports --gogofaster_out=$go_import_paths,import_path=atomix/storage/protocol/gossip,plugins=grpc:pkg   pkg/atomix/storage/protocol/gossip/protocol.proto

protoc -I=$proto_imports --gogofaster_out=$go_import_paths,import_path=atomix/storage/protocol/rsm/counter:pkg    pkg/atomix/storage/protocol/rsm/counter/*.proto
protoc -I=$proto_imports --gogofaster_out=$go_import_paths,import_path=atomix/storage/protocol/rsm/election:pkg   pkg/atomix/storage/protocol/rsm/election/*.proto
protoc -I=$proto_imports --gogofaster_out=$go_import_paths,import_path=atomix/storage/protocol/rsm/indexedmap:pkg pkg/atomix/storage/protocol/rsm/indexedmap/*.proto
protoc -I=$proto_imports --gogofaster_out=$go_import_paths,import_path=atomix/storage/protocol/rsm/list:pkg       pkg/atomix/storage/protocol/rsm/list/*.proto
protoc -I=$proto_imports --gogofaster_out=$go_import_paths,import_path=atomix/storage/protocol/rsm/lock:pkg       pkg/atomix/storage/protocol/rsm/lock/*.proto
protoc -I=$proto_imports --gogofaster_out=$go_import_paths,import_path=atomix/storage/protocol/rsm/map:pkg        pkg/atomix/storage/protocol/rsm/map/*.proto
protoc -I=$proto_imports --gogofaster_out=$go_import_paths,import_path=atomix/storage/protocol/rsm/set:pkg        pkg/atomix/storage/protocol/rsm/set/*.proto
protoc -I=$proto_imports --gogofaster_out=$go_import_paths,import_path=atomix/storage/protocol/rsm/value:pkg      pkg/atomix/storage/protocol/rsm/value/*.proto

protoc -I=$proto_imports --gogofaster_out=$go_import_paths,Matomix/storage/protocol/gossip/primitive/extensions.proto=github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip/primitive,import_path=atomix/storage/protocol/gossip/counter,plugins=grpc:pkg pkg/atomix/storage/protocol/gossip/counter/*.proto
protoc -I=$proto_imports --gogofaster_out=$go_import_paths,Matomix/storage/protocol/gossip/primitive/extensions.proto=github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip/primitive,import_path=atomix/storage/protocol/gossip/map,plugins=grpc:pkg     pkg/atomix/storage/protocol/gossip/map/*.proto
protoc -I=$proto_imports --gogofaster_out=$go_import_paths,Matomix/storage/protocol/gossip/primitive/extensions.proto=github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip/primitive,import_path=atomix/storage/protocol/gossip/set,plugins=grpc:pkg     pkg/atomix/storage/protocol/gossip/set/*.proto
protoc -I=$proto_imports --gogofaster_out=$go_import_paths,Matomix/storage/protocol/gossip/primitive/extensions.proto=github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip/primitive,import_path=atomix/storage/protocol/gossip/value,plugins=grpc:pkg   pkg/atomix/storage/protocol/gossip/value/*.proto
