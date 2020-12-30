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

package leader

import (
	"context"
	storageapi "github.com/atomix/api/go/atomix/storage"
	api "github.com/atomix/api/go/atomix/storage/leader"
	"github.com/atomix/api/go/atomix/storage/timestamp"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "leader")

// RegisterPrimitive registers the election primitive on the given node
func RegisterServer(node *proxy.Node) {
	node.RegisterServer(Type, &ServerType{})
}

// ServerType is the election primitive server
type ServerType struct{}

// RegisterServer registers the election server with the protocol
func (p *ServerType) RegisterServer(server *grpc.Server, client *proxy.Client) {
	api.RegisterLeaderLatchServiceServer(server, &Server{
		Proxy: proxy.NewProxy(client),
	})
}

var _ proxy.PrimitiveServer = &ServerType{}

// Server is an implementation of LeaderElectionServiceServer for the election primitive
type Server struct {
	*proxy.Proxy
}

// Latch enters a candidate in the election
func (s *Server) Latch(ctx context.Context, request *api.LatchRequest) (*api.LatchResponse, error) {
	log.Debugf("Received EnterRequest %+v", request)
	in, err := proto.Marshal(&LatchRequest{
		ID: request.ParticipantID,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opLatch, in, request.Header)
	if err != nil {
		return nil, err
	}

	enterResponse := &LatchResponse{}
	if err = proto.Unmarshal(out, enterResponse); err != nil {
		return nil, err
	}

	response := &api.LatchResponse{
		Latch: &api.Latch{
			ID: timestamp.Epoch{
				Value: enterResponse.Latch.ID,
			},
			Leader:       enterResponse.Latch.Leader,
			Participants: enterResponse.Latch.Participants,
		},
	}
	log.Debugf("Sending EnterResponse %+v", response)
	return response, nil
}

// Get gets the current latch
func (s *Server) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Debugf("Received GetRequest %+v", request)
	in, err := proto.Marshal(&GetRequest{})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoQuery(ctx, opGetLatch, in, request.Header)
	if err != nil {
		return nil, err
	}

	getResponse := &GetResponse{}
	if err = proto.Unmarshal(out, getResponse); err != nil {
		return nil, err
	}

	response := &api.GetResponse{
		Latch: &api.Latch{
			ID: timestamp.Epoch{
				Value: getResponse.Latch.ID,
			},
			Leader:       getResponse.Latch.Leader,
			Participants: getResponse.Latch.Participants,
		},
	}
	log.Debugf("Sending GetTermResponse %+v", response)
	return response, nil
}

// Events lists for election change events
func (s *Server) Events(request *api.EventRequest, srv api.LeaderLatchService_EventsServer) error {
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
		output := result.Value.(proxy.SessionOutput)
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
				Type: api.EventResponse_CHANGED,
				Latch: &api.Latch{
					ID: timestamp.Epoch{
						Value: response.Latch.ID,
					},
					Leader:       response.Latch.Leader,
					Participants: response.Latch.Participants,
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

// Create opens a new session
func (s *Server) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
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
func (s *Server) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
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
