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

package set

import (
	"context"
	storageapi "github.com/atomix/api/go/atomix/storage"
	api "github.com/atomix/api/go/atomix/storage/set"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "set")

// RegisterRSMProxy registers the election primitive on the given node
func RegisterRSMProxy(node *rsm.Node) {
	node.RegisterProxy(Type, func(server *grpc.Server, client *rsm.Client) {
		api.RegisterSetServiceServer(server, &RSMProxy{
			Proxy: rsm.NewProxy(client),
		})
	})
}

// RSMProxy is an implementation of SetServiceServer for the set primitive
type RSMProxy struct {
	*rsm.Proxy
}

// Size gets the number of elements in the set
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

// Contains checks whether the set contains an element
func (s *RSMProxy) Contains(ctx context.Context, request *api.ContainsRequest) (*api.ContainsResponse, error) {
	log.Debugf("Received ContainsRequest %+v", request)
	in, err := proto.Marshal(&ContainsRequest{
		Value: request.Value,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionBy([]byte(request.Value))
	out, err := partition.DoQuery(ctx, opContains, in, request.Header)
	if err != nil {
		return nil, err
	}

	containsResponse := &ContainsResponse{}
	if err = proto.Unmarshal(out, containsResponse); err != nil {
		return nil, err
	}

	response := &api.ContainsResponse{
		Contains: containsResponse.Contains,
	}
	log.Debugf("Sending ContainsResponse %+v", response)
	return response, nil
}

// Add adds an element to the set
func (s *RSMProxy) Add(ctx context.Context, request *api.AddRequest) (*api.AddResponse, error) {
	log.Debugf("Received AddRequest %+v", request)
	in, err := proto.Marshal(&AddRequest{
		Value: request.Value,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionBy([]byte(request.Value))
	out, err := partition.DoCommand(ctx, opAdd, in, request.Header)
	if err != nil {
		return nil, err
	}

	addResponse := &AddResponse{}
	if err = proto.Unmarshal(out, addResponse); err != nil {
		return nil, err
	}

	response := &api.AddResponse{
		Added: addResponse.Added,
	}
	log.Debugf("Sending AddResponse %+v", response)
	return response, nil
}

// Remove removes an element from the set
func (s *RSMProxy) Remove(ctx context.Context, request *api.RemoveRequest) (*api.RemoveResponse, error) {
	log.Debugf("Received RemoveRequest %+v", request)
	in, err := proto.Marshal(&RemoveRequest{
		Value: request.Value,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionBy([]byte(request.Value))
	out, err := partition.DoCommand(ctx, opRemove, in, request.Header)
	if err != nil {
		return nil, err
	}

	removeResponse := &RemoveResponse{}
	if err = proto.Unmarshal(out, removeResponse); err != nil {
		return nil, err
	}

	response := &api.RemoveResponse{
		Removed: removeResponse.Removed,
	}
	log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

// Clear removes all elements from the set
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

// Events listens for set change events
func (s *RSMProxy) Events(request *api.EventRequest, srv api.SetService_EventsServer) error {
	log.Debugf("Received EventRequest %+v", request)
	in, err := proto.Marshal(&ListenRequest{
		Replay: request.Replay,
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
				Type:  getEventType(response.Type),
				Value: response.Value,
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

// Iterate lists all elements currently in the set
func (s *RSMProxy) Iterate(request *api.IterateRequest, srv api.SetService_IterateServer) error {
	log.Debugf("Received IterateRequest %+v", request)
	in, err := proto.Marshal(&IterateRequest{})
	if err != nil {
		return err
	}

	stream := streams.NewBufferedStream()
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoQueryStream(srv.Context(), opIterate, in, request.Header, stream)
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

		response := &IterateResponse{}
		output := result.Value.(rsm.SessionOutput)
		if err = proto.Unmarshal(output.Value.([]byte), response); err != nil {
			return err
		}

		var iterateResponse *api.IterateResponse
		switch output.Type {
		case storageapi.ResponseType_OPEN_STREAM:
			iterateResponse = &api.IterateResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_OPEN_STREAM,
				},
			}
		case storageapi.ResponseType_CLOSE_STREAM:
			iterateResponse = &api.IterateResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_CLOSE_STREAM,
				},
			}
		default:
			iterateResponse = &api.IterateResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_RESPONSE,
				},
				Value: response.Value,
			}
		}

		log.Debugf("Sending IterateResponse %+v", iterateResponse)
		if err = srv.Send(iterateResponse); err != nil {
			return err
		}
	}
	log.Debugf("Finished IterateRequest %+v", request)
	return nil
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

func getEventType(eventType ListenResponse_Type) api.EventResponse_Type {
	switch eventType {
	case ListenResponse_NONE:
		return api.EventResponse_NONE
	case ListenResponse_ADDED:
		return api.EventResponse_ADDED
	case ListenResponse_REMOVED:
		return api.EventResponse_REMOVED
	}
	return api.EventResponse_NONE
}
