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
	"github.com/atomix/atomix-api/proto/atomix/headers"
	api "github.com/atomix/atomix-api/proto/atomix/leader"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-go-node/pkg/atomix/server"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func init() {
	node.RegisterServer(registerServer)
}

// registerServer registers an election server with the given gRPC server
func registerServer(server *grpc.Server, protocol node.Protocol) {
	api.RegisterLeaderLatchServiceServer(server, newServer(protocol.Client()))
}

func newServer(client node.Client) api.LeaderLatchServiceServer {
	return &Server{
		SessionizedServer: &server.SessionizedServer{
			Type:   leaderLatchType,
			Client: client,
		},
	}
}

// Server is an implementation of LeaderElectionServiceServer for the election primitive
type Server struct {
	*server.SessionizedServer
}

// Latch enters a candidate in the election
func (s *Server) Latch(ctx context.Context, request *api.LatchRequest) (*api.LatchResponse, error) {
	log.Tracef("Received EnterRequest %+v", request)
	in, err := proto.Marshal(&LatchRequest{
		ID: request.ParticipantID,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.Command(ctx, opLatch, in, request.Header)
	if err != nil {
		return nil, err
	}

	enterResponse := &LatchResponse{}
	if err = proto.Unmarshal(out, enterResponse); err != nil {
		return nil, err
	}

	response := &api.LatchResponse{
		Header: header,
		Latch: &api.Latch{
			ID:           enterResponse.Latch.ID,
			Leader:       enterResponse.Latch.Leader,
			Participants: enterResponse.Latch.Participants,
		},
	}
	log.Tracef("Sending EnterResponse %+v", response)
	return response, nil
}

// Get gets the current latch
func (s *Server) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Tracef("Received GetRequest %+v", request)
	in, err := proto.Marshal(&GetRequest{})
	if err != nil {
		return nil, err
	}

	out, header, err := s.Query(ctx, opGetLatch, in, request.Header)
	if err != nil {
		return nil, err
	}

	getResponse := &GetResponse{}
	if err = proto.Unmarshal(out, getResponse); err != nil {
		return nil, err
	}

	response := &api.GetResponse{
		Header: header,
		Latch: &api.Latch{
			ID:           getResponse.Latch.ID,
			Leader:       getResponse.Latch.Leader,
			Participants: getResponse.Latch.Participants,
		},
	}
	log.Tracef("Sending GetTermResponse %+v", response)
	return response, nil
}

// Events lists for election change events
func (s *Server) Events(request *api.EventRequest, stream api.LeaderLatchService_EventsServer) error {
	log.Tracef("Received EventRequest %+v", request)
	in, err := proto.Marshal(&ListenRequest{})
	if err != nil {
		return err
	}

	ch := make(chan server.SessionOutput)
	if err := s.CommandStream(opEvents, in, request.Header, ch); err != nil {
		return err
	}

	for result := range ch {
		if result.Failed() {
			return result.Error
		}

		response := &ListenResponse{}
		if err = proto.Unmarshal(result.Value, response); err != nil {
			return err
		}

		var eventResponse *api.EventResponse
		if response.Type == ListenResponse_OPEN {
			eventResponse = &api.EventResponse{
				Header: result.Header,
				Type:   api.EventResponse_OPEN,
			}
		} else {
			eventResponse = &api.EventResponse{
				Header: result.Header,
				Type:   api.EventResponse_CHANGED,
				Latch: &api.Latch{
					ID:           response.Latch.ID,
					Leader:       response.Latch.Leader,
					Participants: response.Latch.Participants,
				},
			}
		}
		log.Tracef("Sending EventResponse %+v", response)
		if err = stream.Send(eventResponse); err != nil {
			return err
		}
	}
	log.Tracef("Finished EventRequest %+v", request)
	return nil
}

// Create opens a new session
func (s *Server) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	log.Tracef("Received CreateRequest %+v", request)
	session, err := s.OpenSession(ctx, request.Header, request.Timeout)
	if err != nil {
		return nil, err
	}
	response := &api.CreateResponse{
		Header: &headers.ResponseHeader{
			SessionID: session,
			Index:     session,
		},
	}
	log.Tracef("Sending CreateResponse %+v", response)
	return response, nil
}

// KeepAlive keeps an existing session alive
func (s *Server) KeepAlive(ctx context.Context, request *api.KeepAliveRequest) (*api.KeepAliveResponse, error) {
	log.Tracef("Received KeepAliveRequest %+v", request)
	if err := s.KeepAliveSession(ctx, request.Header); err != nil {
		return nil, err
	}
	response := &api.KeepAliveResponse{
		Header: &headers.ResponseHeader{
			SessionID: request.Header.SessionID,
		},
	}
	log.Tracef("Sending KeepAliveResponse %+v", response)
	return response, nil
}

// Close closes a session
func (s *Server) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
	log.Tracef("Received CloseRequest %+v", request)
	if request.Delete {
		if err := s.Delete(ctx, request.Header); err != nil {
			return nil, err
		}
	} else {
		if err := s.CloseSession(ctx, request.Header); err != nil {
			return nil, err
		}
	}

	response := &api.CloseResponse{
		Header: &headers.ResponseHeader{
			SessionID: request.Header.SessionID,
		},
	}
	log.Tracef("Sending CloseResponse %+v", response)
	return response, nil
}
