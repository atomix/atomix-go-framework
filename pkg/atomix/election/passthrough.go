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
	api "github.com/atomix/api/go/atomix/storage/election"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
	"google.golang.org/grpc"
	"io"
)

// RegisterPassthroughProxy registers the election primitive on the given node
func RegisterPassthroughProxy(node *passthrough.Node) {
	node.RegisterProxy(Type, func(server *grpc.Server, client *passthrough.Client) {
		api.RegisterLeaderElectionServiceServer(server, &PassthroughProxy{
			Proxy: passthrough.NewProxy(client),
		})
	})
}

// PassthroughProxy is an implementation of LeaderElectionServiceServer for the election primitive
type PassthroughProxy struct {
	*passthrough.Proxy
}

// Enter enters a candidate in the election
func (s *PassthroughProxy) Enter(ctx context.Context, request *api.EnterRequest) (*api.EnterResponse, error) {
	log.Debugf("Received EnterRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLeaderElectionServiceClient(conn)
	response, err := client.Enter(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending EnterResponse %+v", response)
	return response, nil
}

// Withdraw withdraws a candidate from the election
func (s *PassthroughProxy) Withdraw(ctx context.Context, request *api.WithdrawRequest) (*api.WithdrawResponse, error) {
	log.Debugf("Received WithdrawRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLeaderElectionServiceClient(conn)
	response, err := client.Withdraw(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending WithdrawResponse %+v", response)
	return response, nil
}

// Anoint assigns leadership to a candidate
func (s *PassthroughProxy) Anoint(ctx context.Context, request *api.AnointRequest) (*api.AnointResponse, error) {
	log.Debugf("Received AnointRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLeaderElectionServiceClient(conn)
	response, err := client.Anoint(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending AnointResponse %+v", response)
	return response, nil
}

// Promote increases the priority of a candidate
func (s *PassthroughProxy) Promote(ctx context.Context, request *api.PromoteRequest) (*api.PromoteResponse, error) {
	log.Debugf("Received PromoteRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLeaderElectionServiceClient(conn)
	response, err := client.Promote(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending PromoteResponse %+v", response)
	return response, nil
}

// Evict removes a candidate from the election
func (s *PassthroughProxy) Evict(ctx context.Context, request *api.EvictRequest) (*api.EvictResponse, error) {
	log.Debugf("Received EvictRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLeaderElectionServiceClient(conn)
	response, err := client.Evict(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending EvictResponse %+v", response)
	return response, nil
}

// GetTerm gets the current election term
func (s *PassthroughProxy) GetTerm(ctx context.Context, request *api.GetTermRequest) (*api.GetTermResponse, error) {
	log.Debugf("Received GetTermRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLeaderElectionServiceClient(conn)
	response, err := client.GetTerm(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending GetTermResponse %+v", response)
	return response, nil
}

// Events lists for election change events
func (s *PassthroughProxy) Events(request *api.EventRequest, srv api.LeaderElectionService_EventsServer) error {
	log.Debugf("Received EventRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return err
	}

	client := api.NewLeaderElectionServiceClient(conn)
	stream, err := client.Events(srv.Context(), request)
	if err != nil {
		return err
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			log.Debugf("Finished EventRequest %+v", request)
			return nil
		} else if err != nil {
			return err
		}
		log.Debugf("Sending EventResponse %+v", response)
		if err := srv.Send(response); err != nil {
			return err
		}
	}
}

// Create opens a new session
func (s *PassthroughProxy) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	log.Debugf("Received CreateRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLeaderElectionServiceClient(conn)
	response, err := client.Create(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending CreateResponse %+v", response)
	return response, nil
}

// Close closes a session
func (s *PassthroughProxy) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
	log.Debugf("Received CloseRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLeaderElectionServiceClient(conn)
	response, err := client.Close(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}
