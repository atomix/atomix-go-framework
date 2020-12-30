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

package value

import (
	"context"
	storageapi "github.com/atomix/api/go/atomix/storage"
	api "github.com/atomix/api/go/atomix/storage/value"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "value")

// RegisterRSMProxy registers the election primitive on the given node
func RegisterRSMProxy(node *rsm.Node) {
	node.RegisterProxy(Type, func(server *grpc.Server, client *rsm.Client) {
		api.RegisterValueServiceServer(server, &RSMProxy{
			Proxy: rsm.NewProxy(client),
		})
	})
}

// RSMProxy is an implementation of ValueServiceServer for the value primitive
type RSMProxy struct {
	*rsm.Proxy
}

// Set sets the value
func (s *RSMProxy) Set(ctx context.Context, request *api.SetRequest) (*api.SetResponse, error) {
	log.Debugf("Received SetRequest %+v", request)
	in, err := proto.Marshal(&SetRequest{
		ExpectVersion: request.ExpectVersion,
		ExpectValue:   request.ExpectValue,
		Value:         request.Value,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opSet, in, request.Header)
	if err != nil {
		return nil, err
	}

	setResponse := &SetResponse{}
	if err = proto.Unmarshal(out, setResponse); err != nil {
		return nil, err
	}

	response := &api.SetResponse{
		Version:   setResponse.Version,
		Succeeded: setResponse.Succeeded,
	}
	log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

// Get gets the current value and version
func (s *RSMProxy) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Debugf("Received GetRequest %+v", request)
	in, err := proto.Marshal(&GetRequest{})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoQuery(ctx, opGet, in, request.Header)
	if err != nil {
		return nil, err
	}

	getResponse := &GetResponse{}
	if err = proto.Unmarshal(out, getResponse); err != nil {
		return nil, err
	}

	response := &api.GetResponse{
		Value:   getResponse.Value,
		Version: getResponse.Version,
	}
	log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

// Events listens for value change events
func (s *RSMProxy) Events(request *api.EventRequest, srv api.ValueService_EventsServer) error {
	log.Debugf("Received EventRequest %+v", request)
	in, err := proto.Marshal(&ListenRequest{})
	if err != nil {
		return err
	}

	stream := streams.NewBufferedStream()
	partition := s.PartitionFor(request.Header.Primitive)
	if err := partition.DoCommandStream(srv.Context(), opEvents, in, request.Header, stream); err != nil {
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
				Type:            getEventType(response.Type),
				PreviousValue:   response.PreviousValue,
				PreviousVersion: response.PreviousVersion,
				NewValue:        response.NewValue,
				NewVersion:      response.NewVersion,
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

// Create opens a new session
func (s *RSMProxy) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	log.Debugf("Received CreateRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	err := partition.DoCreateService(ctx, request.Header)
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
		partition := s.PartitionFor(request.Header.Primitive)
		err := partition.DoDeleteService(ctx, request.Header)
		if err != nil {
			return nil, err
		}
		response := &api.CloseResponse{}
		log.Debugf("Sending CloseResponse %+v", response)
		return response, nil
	}

	partition := s.PartitionFor(request.Header.Primitive)
	err := partition.DoCloseService(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	response := &api.CloseResponse{}
	log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}

func getEventType(eventType ListenResponse_Type) api.EventResponse_Type {
	switch eventType {
	case ListenResponse_UPDATED:
		return api.EventResponse_UPDATED
	}
	return api.EventResponse_UPDATED
}
