#!/bin/sh

proto_path="/go/src/github.com/atomix/api/proto:${GOPATH}/src/github.com/gogo/protobuf:${GOPATH}/src/github.com/gogo/protobuf/protobuf:${GOPATH}/src"

go_import_paths="Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types"
go_import_paths="${go_import_paths},Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types"
go_import_paths="${go_import_paths},Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types"
go_import_paths="${go_import_paths},Mgoogle/protobuf/descriptor.proto=github.com/golang/protobuf/protoc-gen-go/descriptor"
go_import_paths="${go_import_paths},Matomix/protocol/protocol.proto=github.com/atomix/api/go/atomix/protocol"
go_import_paths="${go_import_paths},Matomix/primitive/primitive.proto=github.com/atomix/api/go/atomix/primitive"
go_import_paths="${go_import_paths},Matomix/primitive/timestamp/timestamp.proto=github.com/atomix/api/go/atomix/primitive/timestamp"

protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/counter,output_path=atomix/proxy/rsm/counter,plugin=proxy,protocol=rsm:pkg       /go/src/github.com/atomix/api/proto/atomix/primitive/counter/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/election,output_path=atomix/proxy/rsm/election,plugin=proxy,protocol=rsm:pkg     /go/src/github.com/atomix/api/proto/atomix/primitive/election/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/indexedmap,output_path=atomix/proxy/rsm/indexedmap,plugin=proxy,protocol=rsm:pkg /go/src/github.com/atomix/api/proto/atomix/primitive/indexedmap/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/leader,output_path=atomix/proxy/rsm/leader,plugin=proxy,protocol=rsm:pkg         /go/src/github.com/atomix/api/proto/atomix/primitive/leader/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/list,output_path=atomix/proxy/rsm/list,plugin=proxy,protocol=rsm:pkg             /go/src/github.com/atomix/api/proto/atomix/primitive/list/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/lock,output_path=atomix/proxy/rsm/lock,plugin=proxy,protocol=rsm:pkg             /go/src/github.com/atomix/api/proto/atomix/primitive/lock/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/log,output_path=atomix/proxy/rsm/log,plugin=proxy,protocol=rsm:pkg               /go/src/github.com/atomix/api/proto/atomix/primitive/log/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/map,output_path=atomix/proxy/rsm/map,plugin=proxy,protocol=rsm:pkg               /go/src/github.com/atomix/api/proto/atomix/primitive/map/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/set,output_path=atomix/proxy/rsm/set,plugin=proxy,protocol=rsm:pkg               /go/src/github.com/atomix/api/proto/atomix/primitive/set/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/value,output_path=atomix/proxy/rsm/value,plugin=proxy,protocol=rsm:pkg           /go/src/github.com/atomix/api/proto/atomix/primitive/value/*.proto

protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/counter,output_path=atomix/protocol/rsm/counter,plugin=protocol,protocol=rsm:pkg       /go/src/github.com/atomix/api/proto/atomix/primitive/counter/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/election,output_path=atomix/protocol/rsm/election,plugin=protocol,protocol=rsm:pkg     /go/src/github.com/atomix/api/proto/atomix/primitive/election/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/indexedmap,output_path=atomix/protocol/rsm/indexedmap,plugin=protocol,protocol=rsm:pkg /go/src/github.com/atomix/api/proto/atomix/primitive/indexedmap/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/leader,output_path=atomix/protocol/rsm/leader,plugin=protocol,protocol=rsm:pkg         /go/src/github.com/atomix/api/proto/atomix/primitive/leader/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/list,output_path=atomix/protocol/rsm/list,plugin=protocol,protocol=rsm:pkg             /go/src/github.com/atomix/api/proto/atomix/primitive/list/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/lock,output_path=atomix/protocol/rsm/lock,plugin=protocol,protocol=rsm:pkg             /go/src/github.com/atomix/api/proto/atomix/primitive/lock/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/log,output_path=atomix/protocol/rsm/log,plugin=protocol,protocol=rsm:pkg               /go/src/github.com/atomix/api/proto/atomix/primitive/log/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/map,output_path=atomix/protocol/rsm/map,plugin=protocol,protocol=rsm:pkg               /go/src/github.com/atomix/api/proto/atomix/primitive/map/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/set,output_path=atomix/protocol/rsm/set,plugin=protocol,protocol=rsm:pkg               /go/src/github.com/atomix/api/proto/atomix/primitive/set/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/value,output_path=atomix/protocol/rsm/value,plugin=protocol,protocol=rsm:pkg           /go/src/github.com/atomix/api/proto/atomix/primitive/value/*.proto

protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/counter,output_path=atomix/proxy/crdt/counter,plugin=proxy,protocol=crdt:pkg /go/src/github.com/atomix/api/proto/atomix/primitive/counter/*.proto

protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/counter,output_path=atomix/protocol/crdt/counter,plugin=protocol,protocol=crdt:pkg /go/src/github.com/atomix/api/proto/atomix/primitive/counter/*.proto

protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/map,output_path=atomix/proxy/gossip/map,plugin=proxy,protocol=gossip:pkg         /go/src/github.com/atomix/api/proto/atomix/primitive/map/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/value,output_path=atomix/proxy/gossip/value,plugin=proxy,protocol=gossip:pkg     /go/src/github.com/atomix/api/proto/atomix/primitive/value/*.proto

protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/map,output_path=atomix/protocol/gossip/map,plugin=protocol,protocol=gossip:pkg         /go/src/github.com/atomix/api/proto/atomix/primitive/map/*.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/api/go/atomix/primitive/value,output_path=atomix/protocol/gossip/value,plugin=protocol,protocol=gossip:pkg     /go/src/github.com/atomix/api/proto/atomix/primitive/value/*.proto
