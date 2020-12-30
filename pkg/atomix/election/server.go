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

package election

import (
	"context"
	storageapi "github.com/atomix/api/go/atomix/storage"
	api "github.com/atomix/api/go/atomix/storage/election"
	"github.com/atomix/api/go/atomix/storage/timestamp"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "election")

// RegisterPrimitive registers the election primitive on the given node
func RegisterServer(node *proxy.Node) {
	node.RegisterServer(Type, &ServerType{})
}

// ServerType is the election primitive server
type ServerType struct{}

// RegisterServer registers the election server with the protocol
func (p *ServerType) RegisterServer(server *grpc.Server, client *proxy.Client) {
	api.RegisterLeaderElectionServiceServer(server, &Server{
		Proxy: proxy.NewProxy(client),
	})
}

var _ proxy.PrimitiveServer = &ServerType{}

// Server is an implementation of LeaderElectionServiceServer for the election primitive
type Server struct {
	*proxy.Proxy
}

// Enter enters a candidate in the election
func (s *Server) Enter(ctx context.Context, request *api.EnterRequest) (*api.EnterResponse, error) {
	log.Debugf("Received EnterRequest %+v", request)
	in, err := proto.Marshal(&EnterRequest{
		ID: request.CandidateID,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opEnter, in, request.Header)
	if err != nil {
		return nil, err
	}

	enterResponse := &EnterResponse{}
	if err = proto.Unmarshal(out, enterResponse); err != nil {
		return nil, err
	}

	response := &api.EnterResponse{
		Term: &api.Term{
			ID: timestamp.Epoch{
				Value: enterResponse.Term.ID,
			},
			Timestamp:  enterResponse.Term.Timestamp,
			Leader:     enterResponse.Term.Leader,
			Candidates: enterResponse.Term.Candidates,
		},
	}
	log.Debugf("Sending EnterResponse %+v", response)
	return response, nil
}

// Withdraw withdraws a candidate from the election
func (s *Server) Withdraw(ctx context.Context, request *api.WithdrawRequest) (*api.WithdrawResponse, error) {
	log.Debugf("Received WithdrawRequest %+v", request)
	in, err := proto.Marshal(&WithdrawRequest{
		ID: request.CandidateID,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opWithdraw, in, request.Header)
	if err != nil {
		return nil, err
	}

	withdrawResponse := &WithdrawResponse{}
	if err = proto.Unmarshal(out, withdrawResponse); err != nil {
		return nil, err
	}

	response := &api.WithdrawResponse{
		Term: &api.Term{
			ID: timestamp.Epoch{
				Value: withdrawResponse.Term.ID,
			},
			Timestamp:  withdrawResponse.Term.Timestamp,
			Leader:     withdrawResponse.Term.Leader,
			Candidates: withdrawResponse.Term.Candidates,
		},
	}
	log.Debugf("Sending WithdrawResponse %+v", response)
	return response, nil
}

// Anoint assigns leadership to a candidate
func (s *Server) Anoint(ctx context.Context, request *api.AnointRequest) (*api.AnointResponse, error) {
	log.Debugf("Received AnointRequest %+v", request)
	in, err := proto.Marshal(&AnointRequest{
		ID: request.CandidateID,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opAnoint, in, request.Header)
	if err != nil {
		return nil, err
	}

	anointResponse := &AnointResponse{}
	if err = proto.Unmarshal(out, anointResponse); err != nil {
		return nil, err
	}

	response := &api.AnointResponse{
		Term: &api.Term{
			ID: timestamp.Epoch{
				Value: anointResponse.Term.ID,
			},
			Timestamp:  anointResponse.Term.Timestamp,
			Leader:     anointResponse.Term.Leader,
			Candidates: anointResponse.Term.Candidates,
		},
	}
	log.Debugf("Sending AnointResponse %+v", response)
	return response, nil
}

// Promote increases the priority of a candidate
func (s *Server) Promote(ctx context.Context, request *api.PromoteRequest) (*api.PromoteResponse, error) {
	log.Debugf("Received PromoteRequest %+v", request)
	in, err := proto.Marshal(&PromoteRequest{
		ID: request.CandidateID,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opPromote, in, request.Header)
	if err != nil {
		return nil, err
	}

	promoteResponse := &PromoteResponse{}
	if err = proto.Unmarshal(out, promoteResponse); err != nil {
		return nil, err
	}

	response := &api.PromoteResponse{
		Term: &api.Term{
			ID: timestamp.Epoch{
				Value: promoteResponse.Term.ID,
			},
			Timestamp:  promoteResponse.Term.Timestamp,
			Leader:     promoteResponse.Term.Leader,
			Candidates: promoteResponse.Term.Candidates,
		},
	}
	log.Debugf("Sending PromoteResponse %+v", response)
	return response, nil
}

// Evict removes a candidate from the election
func (s *Server) Evict(ctx context.Context, request *api.EvictRequest) (*api.EvictResponse, error) {
	log.Debugf("Received EvictRequest %+v", request)
	in, err := proto.Marshal(&EvictRequest{
		ID: request.CandidateID,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opEvict, in, request.Header)
	if err != nil {
		return nil, err
	}

	evictResponse := &EvictResponse{}
	if err = proto.Unmarshal(out, evictResponse); err != nil {
		return nil, err
	}

	response := &api.EvictResponse{
		Term: &api.Term{
			ID: timestamp.Epoch{
				Value: evictResponse.Term.ID,
			},
			Timestamp:  evictResponse.Term.Timestamp,
			Leader:     evictResponse.Term.Leader,
			Candidates: evictResponse.Term.Candidates,
		},
	}
	log.Debugf("Sending EvictResponse %+v", response)
	return response, nil
}

// GetTerm gets the current election term
func (s *Server) GetTerm(ctx context.Context, request *api.GetTermRequest) (*api.GetTermResponse, error) {
	log.Debugf("Received GetTermRequest %+v", request)
	in, err := proto.Marshal(&GetTermRequest{})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoQuery(ctx, opGetTerm, in, request.Header)
	if err != nil {
		return nil, err
	}

	getResponse := &GetTermResponse{}
	if err = proto.Unmarshal(out, getResponse); err != nil {
		return nil, err
	}

	response := &api.GetTermResponse{
		Term: &api.Term{
			ID: timestamp.Epoch{
				Value: getResponse.Term.ID,
			},
			Timestamp:  getResponse.Term.Timestamp,
			Leader:     getResponse.Term.Leader,
			Candidates: getResponse.Term.Candidates,
		},
	}
	log.Debugf("Sending GetTermResponse %+v", response)
	return response, nil
}

// Events lists for election change events
func (s *Server) Events(request *api.EventRequest, srv api.LeaderElectionService_EventsServer) error {
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
				Term: &api.Term{
					ID: timestamp.Epoch{
						Value: response.Term.ID,
					},
					Timestamp:  response.Term.Timestamp,
					Leader:     response.Term.Leader,
					Candidates: response.Term.Candidates,
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
