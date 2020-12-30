// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package _map //nolint:golint

import (
	"context"
	storageapi "github.com/atomix/api/go/atomix/storage"
	api "github.com/atomix/api/go/atomix/storage/map"
	"github.com/atomix/api/go/atomix/storage/timestamp"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "map")

// RegisterRSMProxy registers the election primitive on the given node
func RegisterRSMProxy(node *rsm.Node) {
	node.RegisterProxy(Type, func(server *grpc.Server, client *rsm.Client) {
		api.RegisterMapServiceServer(server, &RSMProxy{
			Proxy: rsm.NewProxy(client),
		})
	})
}

// RSMProxy is an implementation of MapServiceServer for the map primitive
type RSMProxy struct {
	*rsm.Proxy
}

// Create opens a new session
func (s *RSMProxy) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	log.Debugf("Received CreateRequest %+v", request)
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoCreateService(ctx, request.Header)
	})
	if err != nil {
		return nil, err
	}
	response := &api.CreateResponse{}
	log.Debugf("Sending CreateResponse %+v", response)
	return response, nil
}

// Close closes a session
func (s *RSMProxy) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
	log.Debugf("Received CloseRequest %+v", request)
	if request.Delete {
		partitions := s.Partitions()
		err := async.IterAsync(len(partitions), func(i int) error {
			return partitions[i].DoDeleteService(ctx, request.Header)
		})
		if err != nil {
			return nil, err
		}
		response := &api.CloseResponse{}
		log.Debugf("Sending CloseResponse %+v", response)
		return response, nil
	}

	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoCloseService(ctx, request.Header)
	})
	if err != nil {
		return nil, err
	}
	response := &api.CloseResponse{}
	log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}

// Size gets the number of entries in the map
func (s *RSMProxy) Size(ctx context.Context, request *api.SizeRequest) (*api.SizeResponse, error) {
	log.Debugf("Received SizeRequest %+v", request)
	in, err := proto.Marshal(&SizeRequest{})
	if err != nil {
		return nil, err
	}

	partitions := s.Partitions()
	results, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		out, err := partitions[i].DoQuery(ctx, opSize, in, request.Header)
		if err != nil {
			return nil, err
		}
		sizeResponse := &SizeResponse{}
		if err = proto.Unmarshal(out, sizeResponse); err != nil {
			return nil, err
		}
		return sizeResponse.Size_, nil
	})

	var size uint32
	for _, result := range results {
		size += result.(uint32)
	}

	response := &api.SizeResponse{
		Size_: size,
	}
	log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

// Exists checks whether the map contains a key
func (s *RSMProxy) Exists(ctx context.Context, request *api.ExistsRequest) (*api.ExistsResponse, error) {
	log.Debugf("Received ExistsRequest %+v", request)
	in, err := proto.Marshal(&ContainsKeyRequest{
		Key: request.Key,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionBy([]byte(request.Key))
	out, err := partition.DoQuery(ctx, opExists, in, request.Header)
	if err != nil {
		return nil, err
	}

	containsResponse := &ContainsKeyResponse{}
	if err = proto.Unmarshal(out, containsResponse); err != nil {
		return nil, err
	}

	response := &api.ExistsResponse{
		ContainsKey: containsResponse.ContainsKey,
	}
	log.Debugf("Sending ExistsResponse %+v", response)
	return response, nil
}

// Put puts a key/value pair into the map
func (s *RSMProxy) Put(ctx context.Context, request *api.PutRequest) (*api.PutResponse, error) {
	log.Debugf("Received PutRequest %+v", request)
	in, err := proto.Marshal(&PutRequest{
		Key:     request.Key,
		Value:   request.Value,
		Version: request.Version,
		TTL:     request.TTL,
		IfEmpty: request.IfEmpty,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionBy([]byte(request.Key))
	out, err := partition.DoCommand(ctx, opPut, in, request.Header)
	if err != nil {
		return nil, err
	}

	putResponse := &PutResponse{}
	if err = proto.Unmarshal(out, putResponse); err != nil {
		return nil, err
	}

	response := &api.PutResponse{
		Entry: &api.Entry{
			Key:   request.Key,
			Value: putResponse.PreviousValue,
			Timestamp: timestamp.Timestamp{
				Timestamp: &timestamp.Timestamp_LogicalTimestamp{
					LogicalTimestamp: &timestamp.LogicalTimestamp{
						Value: putResponse.PreviousVersion,
					},
				},
			},
			Created: putResponse.Created,
			Updated: putResponse.Updated,
		},
	}
	log.Debugf("Sending PutResponse %+v", response)
	return response, nil
}

// Get gets the value of a key
func (s *RSMProxy) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Debugf("Received GetRequest %+v", request)
	in, err := proto.Marshal(&GetRequest{
		Key: request.Key,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionBy([]byte(request.Key))
	out, err := partition.DoQuery(ctx, opGet, in, request.Header)
	if err != nil {
		return nil, err
	}

	getResponse := &GetResponse{}
	if err = proto.Unmarshal(out, getResponse); err != nil {
		return nil, err
	}

	response := &api.GetResponse{
		Entry: &api.Entry{
			Key:   request.Key,
			Value: getResponse.Value,
			Timestamp: timestamp.Timestamp{
				Timestamp: &timestamp.Timestamp_LogicalTimestamp{
					LogicalTimestamp: &timestamp.LogicalTimestamp{
						Value: getResponse.Version,
					},
				},
			},
			Created: getResponse.Created,
			Updated: getResponse.Updated,
		},
	}
	log.Debugf("Sending GetRequest %+v", response)
	return response, nil
}

// Remove removes a key from the map
func (s *RSMProxy) Remove(ctx context.Context, request *api.RemoveRequest) (*api.RemoveResponse, error) {
	log.Debugf("Received RemoveRequest %+v", request)
	var version uint64
	if request.Timestamp != nil {
		version = request.Timestamp.GetLogicalTimestamp().Value
	}
	in, err := proto.Marshal(&RemoveRequest{
		Key:     request.Key,
		Version: version,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionBy([]byte(request.Key))
	out, err := partition.DoCommand(ctx, opRemove, in, request.Header)
	if err != nil {
		return nil, err
	}

	serviceResponse := &RemoveResponse{}
	if err = proto.Unmarshal(out, serviceResponse); err != nil {
		return nil, err
	}

	response := &api.RemoveResponse{
		Entry: &api.Entry{
			Key:   request.Key,
			Value: serviceResponse.PreviousValue,
			Timestamp: timestamp.Timestamp{
				Timestamp: &timestamp.Timestamp_LogicalTimestamp{
					LogicalTimestamp: &timestamp.LogicalTimestamp{
						Value: serviceResponse.PreviousVersion,
					},
				},
			},
			Created: serviceResponse.Created,
			Updated: serviceResponse.Updated,
		},
	}
	log.Debugf("Sending RemoveRequest %+v", response)
	return response, nil
}

// Clear removes all keys from the map
func (s *RSMProxy) Clear(ctx context.Context, request *api.ClearRequest) (*api.ClearResponse, error) {
	log.Debugf("Received ClearRequest %+v", request)
	in, err := proto.Marshal(&ClearRequest{})
	if err != nil {
		return nil, err
	}

	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		_, err := partitions[i].DoCommand(ctx, opClear, in, request.Header)
		return err
	})
	if err != nil {
		return nil, err
	}

	response := &api.ClearResponse{}
	log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

// Events listens for map change events
func (s *RSMProxy) Events(request *api.EventRequest, srv api.MapService_EventsServer) error {
	log.Debugf("Received EventRequest %+v", request)
	in, err := proto.Marshal(&ListenRequest{
		Replay: request.Replay,
		Key:    request.Key,
	})
	if err != nil {
		return err
	}

	stream := streams.NewBufferedStream()
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoCommandStream(srv.Context(), opEvents, in, request.Header, stream)
	})
	if err != nil {
		return err
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			return result.Error
		}

		response := &ListenResponse{}
		output := result.Value.(rsm.SessionOutput)
		if err = proto.Unmarshal(output.Value.([]byte), response); err != nil {
			return err
		}

		var eventResponse *api.EventResponse
		switch output.Type {
		case storageapi.ResponseType_OPEN_STREAM:
			eventResponse = &api.EventResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_OPEN_STREAM,
				},
			}
		case storageapi.ResponseType_CLOSE_STREAM:
			eventResponse = &api.EventResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_CLOSE_STREAM,
				},
			}
		default:
			eventResponse = &api.EventResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_RESPONSE,
				},
				Type: getEventType(response.Type),
				Entry: api.Entry{
					Key:   response.Key,
					Value: response.Value,
					Timestamp: timestamp.Timestamp{
						Timestamp: &timestamp.Timestamp_LogicalTimestamp{
							LogicalTimestamp: &timestamp.LogicalTimestamp{
								Value: response.Version,
							},
						},
					},
					Created: response.Created,
					Updated: response.Updated,
				},
			}
		}

		log.Debugf("Sending EventResponse %+v", eventResponse)
		if err = srv.Send(eventResponse); err != nil {
			return err
		}
	}
	log.Debugf("Finished EventRequest %+v", request)
	return nil
}

// Entries lists all entries currently in the map
func (s *RSMProxy) Entries(request *api.EntriesRequest, srv api.MapService_EntriesServer) error {
	log.Debugf("Received EntriesRequest %+v", request)
	in, err := proto.Marshal(&EntriesRequest{})
	if err != nil {
		return err
	}

	stream := streams.NewBufferedStream()
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoQueryStream(srv.Context(), opEntries, in, request.Header, stream)
	})
	if err != nil {
		return err
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			return result.Error
		}

		response := &EntriesResponse{}
		output := result.Value.(rsm.SessionOutput)
		if err = proto.Unmarshal(output.Value.([]byte), response); err != nil {
			return err
		}

		var entriesResponse *api.EntriesResponse
		switch output.Type {
		case storageapi.ResponseType_OPEN_STREAM:
			entriesResponse = &api.EntriesResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_OPEN_STREAM,
				},
			}
		case storageapi.ResponseType_CLOSE_STREAM:
			entriesResponse = &api.EntriesResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_CLOSE_STREAM,
				},
			}
		default:
			entriesResponse = &api.EntriesResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_RESPONSE,
				},
				Entry: api.Entry{
					Key:   response.Key,
					Value: response.Value,
					Timestamp: timestamp.Timestamp{
						Timestamp: &timestamp.Timestamp_LogicalTimestamp{
							LogicalTimestamp: &timestamp.LogicalTimestamp{
								Value: response.Version,
							},
						},
					},
					Created: response.Created,
					Updated: response.Updated,
				},
			}
		}

		log.Debugf("Sending EntriesResponse %+v", entriesResponse)
		if err = srv.Send(entriesResponse); err != nil {
			return err
		}
	}
	log.Debugf("Finished EntriesRequest %+v", request)
	return nil
}

func getEventType(eventType ListenResponse_Type) api.EventResponse_Type {
	switch eventType {
	case ListenResponse_NONE:
		return api.EventResponse_NONE
	case ListenResponse_INSERTED:
		return api.EventResponse_INSERTED
	case ListenResponse_UPDATED:
		return api.EventResponse_UPDATED
	case ListenResponse_REMOVED:
		return api.EventResponse_REMOVED
	}
	return api.EventResponse_NONE
}
