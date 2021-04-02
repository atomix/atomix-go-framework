#!/bin/sh

proto_path="./pkg:/go/src/github.com/atomix/api/proto:${GOPATH}/src/github.com/gogo/protobuf:${GOPATH}/src/github.com/gogo/protobuf/protobuf:${GOPATH}/src"

go_import_paths="Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types"
go_import_paths="${go_import_paths},Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types"
go_import_paths="${go_import_paths},Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types"
go_import_paths="${go_import_paths},Mgoogle/protobuf/descriptor.proto=github.com/golang/protobuf/protoc-gen-go/descriptor"
go_import_paths="${go_import_paths},Matomix/protocol/protocol.proto=github.com/atomix/api/go/atomix/protocol"
go_import_paths="${go_import_paths},Matomix/primitive/primitive.proto=github.com/atomix/api/go/atomix/primitive"
go_import_paths="${go_import_paths},Matomix/primitive/timestamp/timestamp.proto=github.com/atomix/api/go/atomix/primitive/timestamp"

go_import_paths="${go_import_paths},Matomix/primitive/counter/counter.proto=github.com/atomix/api/go/atomix/primitive/counter"
go_import_paths="${go_import_paths},Matomix/primitive/election/election.proto=github.com/atomix/api/go/atomix/primitive/election"
go_import_paths="${go_import_paths},Matomix/primitive/indexedmap/indexedmap.proto=github.com/atomix/api/go/atomix/primitive/indexedmap"
go_import_paths="${go_import_paths},Matomix/primitive/leader/latch.proto=github.com/atomix/api/go/atomix/primitive/leader"
go_import_paths="${go_import_paths},Matomix/primitive/list/list.proto=github.com/atomix/api/go/atomix/primitive/list"
go_import_paths="${go_import_paths},Matomix/primitive/lock/lock.proto=github.com/atomix/api/go/atomix/primitive/lock"
go_import_paths="${go_import_paths},Matomix/primitive/log/log.proto=github.com/atomix/api/go/atomix/primitive/log"
go_import_paths="${go_import_paths},Matomix/primitive/map/map.proto=github.com/atomix/api/go/atomix/primitive/map"
go_import_paths="${go_import_paths},Matomix/primitive/set/set.proto=github.com/atomix/api/go/atomix/primitive/set"
go_import_paths="${go_import_paths},Matomix/primitive/value/value.proto=github.com/atomix/api/go/atomix/primitive/value"

protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/primitive/counter,plugin=driver,protocol=primitive,output_path=atomix/driver/primitive/counter:pkg       atomix/primitive/counter/counter.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/primitive/election,plugin=driver,protocol=primitive,output_path=atomix/driver/primitive/election:pkg     atomix/primitive/election/election.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/primitive/indexedmap,plugin=driver,protocol=primitive,output_path=atomix/driver/primitive/indexedmap:pkg atomix/primitive/indexedmap/indexedmap.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/primitive/leader,plugin=driver,protocol=primitive,output_path=atomix/driver/primitive/leader:pkg         atomix/primitive/leader/latch.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/primitive/list,plugin=driver,protocol=primitive,output_path=atomix/driver/primitive/list:pkg             atomix/primitive/list/list.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/primitive/lock,plugin=driver,protocol=primitive,output_path=atomix/driver/primitive/lock:pkg             atomix/primitive/lock/lock.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/primitive/log,plugin=driver,protocol=primitive,output_path=atomix/driver/primitive/log:pkg               atomix/primitive/log/log.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/primitive/map,plugin=driver,protocol=primitive,output_path=atomix/driver/primitive/map:pkg               atomix/primitive/map/map.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/primitive/set,plugin=driver,protocol=primitive,output_path=atomix/driver/primitive/set:pkg               atomix/primitive/set/set.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/primitive/value,plugin=driver,protocol=primitive,output_path=atomix/driver/primitive/value:pkg           atomix/primitive/value/value.proto

protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm/counter,plugin=proxy,protocol=rsm,output_path=atomix/driver/proxy/rsm/counter:pkg       atomix/primitive/counter/counter.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm/election,plugin=proxy,protocol=rsm,output_path=atomix/driver/proxy/rsm/election:pkg     atomix/primitive/election/election.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm/indexedmap,plugin=proxy,protocol=rsm,output_path=atomix/driver/proxy/rsm/indexedmap:pkg atomix/primitive/indexedmap/indexedmap.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm/leader,plugin=proxy,protocol=rsm,output_path=atomix/driver/proxy/rsm/leader:pkg         atomix/primitive/leader/latch.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm/list,plugin=proxy,protocol=rsm,output_path=atomix/driver/proxy/rsm/list:pkg             atomix/primitive/list/list.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm/lock,plugin=proxy,protocol=rsm,output_path=atomix/driver/proxy/rsm/lock:pkg             atomix/primitive/lock/lock.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm/log,plugin=proxy,protocol=rsm,output_path=atomix/driver/proxy/rsm/log:pkg               atomix/primitive/log/log.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm/map,plugin=proxy,protocol=rsm,output_path=atomix/driver/proxy/rsm/map:pkg               atomix/primitive/map/map.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm/set,plugin=proxy,protocol=rsm,output_path=atomix/driver/proxy/rsm/set:pkg               atomix/primitive/set/set.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm/value,plugin=proxy,protocol=rsm,output_path=atomix/driver/proxy/rsm/value:pkg           atomix/primitive/value/value.proto

protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm/counter,plugin=storage,protocol=rsm,output_path=atomix/storage/protocol/rsm/counter:pkg       atomix/primitive/counter/counter.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm/election,plugin=storage,protocol=rsm,output_path=atomix/storage/protocol/rsm/election:pkg     atomix/primitive/election/election.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm/indexedmap,plugin=storage,protocol=rsm,output_path=atomix/storage/protocol/rsm/indexedmap:pkg atomix/primitive/indexedmap/indexedmap.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm/leader,plugin=storage,protocol=rsm,output_path=atomix/storage/protocol/rsm/leader:pkg         atomix/primitive/leader/latch.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm/list,plugin=storage,protocol=rsm,output_path=atomix/storage/protocol/rsm/list:pkg             atomix/primitive/list/list.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm/lock,plugin=storage,protocol=rsm,output_path=atomix/storage/protocol/rsm/lock:pkg             atomix/primitive/lock/lock.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm/log,plugin=storage,protocol=rsm,output_path=atomix/storage/protocol/rsm/log:pkg               atomix/primitive/log/log.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm/map,plugin=storage,protocol=rsm,output_path=atomix/storage/protocol/rsm/map:pkg               atomix/primitive/map/map.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm/set,plugin=storage,protocol=rsm,output_path=atomix/storage/protocol/rsm/set:pkg               atomix/primitive/set/set.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm/value,plugin=storage,protocol=rsm,output_path=atomix/storage/protocol/rsm/value:pkg           atomix/primitive/value/value.proto

go_import_paths="${go_import_paths},Matomix/storage/protocol/gossip/primitive/extensions.proto=github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip/primitive"

protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/gossip/counter,plugin=proxy,protocol=gossip,output_path=atomix/driver/proxy/gossip/counter:pkg atomix/primitive/counter/counter.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/gossip/map,plugin=proxy,protocol=gossip,output_path=atomix/driver/proxy/gossip/map:pkg         atomix/primitive/map/map.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/gossip/set,plugin=proxy,protocol=gossip,output_path=atomix/driver/proxy/gossip/set:pkg         atomix/primitive/set/set.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/driver/proxy/gossip/value,plugin=proxy,protocol=gossip,output_path=atomix/driver/proxy/gossip/value:pkg     atomix/primitive/value/value.proto

go_import_paths="${go_import_paths},Matomix/storage/protocol/gossip/counter/state.proto=github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip/counter"
go_import_paths="${go_import_paths},Matomix/storage/protocol/gossip/map/state.proto=github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip/map"
go_import_paths="${go_import_paths},Matomix/storage/protocol/gossip/set/state.proto=github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip/set"
go_import_paths="${go_import_paths},Matomix/storage/protocol/gossip/value/state.proto=github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip/value"

protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip/counter,plugin=storage,protocol=gossip,state=discrete,entry=.atomix.storage.protocol.gossip.counter.CounterState,output_path=atomix/storage/protocol/gossip/counter:pkg atomix/primitive/counter/counter.proto atomix/storage/protocol/gossip/counter/state.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip/map,plugin=storage,protocol=gossip,state=continuous,entry=.atomix.storage.protocol.gossip.map.MapEntry,output_path=atomix/storage/protocol/gossip/map:pkg               atomix/primitive/map/map.proto         atomix/storage/protocol/gossip/map/state.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip/set,plugin=storage,protocol=gossip,state=continuous,entry=.atomix.storage.protocol.gossip.set.SetElement,output_path=atomix/storage/protocol/gossip/set:pkg             atomix/primitive/set/set.proto         atomix/storage/protocol/gossip/set/state.proto
protoc -I=$proto_path --atomix_out=$go_import_paths,import_path=github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip/value,plugin=storage,protocol=gossip,state=discrete,entry=.atomix.storage.protocol.gossip.value.ValueState,output_path=atomix/storage/protocol/gossip/value:pkg         atomix/primitive/value/value.proto     atomix/storage/protocol/gossip/value/state.proto
