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
	api "github.com/atomix/api/go/atomix/storage/leader"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
	"google.golang.org/grpc"
	"io"
)

// RegisterPassthroughProxy registers the election primitive on the given node
func RegisterPassthroughProxy(node *passthrough.Node) {
	node.RegisterProxy(Type, func(server *grpc.Server, client *passthrough.Client) {
		api.RegisterLeaderLatchServiceServer(server, &PassthroughProxy{
			Proxy: passthrough.NewProxy(client),
		})
	})
}

// PassthroughProxy is an implementation of LeaderElectionServiceServer for the election primitive
type PassthroughProxy struct {
	*passthrough.Proxy
}

// Latch enters a candidate in the election
func (s *PassthroughProxy) Latch(ctx context.Context, request *api.LatchRequest) (*api.LatchResponse, error) {
	log.Debugf("Received EnterRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLeaderLatchServiceClient(conn)
	response, err := client.Latch(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending EnterResponse %+v", response)
	return response, nil
}

// Get gets the current latch
func (s *PassthroughProxy) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Debugf("Received GetRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLeaderLatchServiceClient(conn)
	response, err := client.Get(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending GetTermResponse %+v", response)
	return response, nil
}

// Events lists for election change events
func (s *PassthroughProxy) Events(request *api.EventRequest, srv api.LeaderLatchService_EventsServer) error {
	log.Debugf("Received EventRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return err
	}

	client := api.NewLeaderLatchServiceClient(conn)
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

	client := api.NewLeaderLatchServiceClient(conn)
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

	client := api.NewLeaderLatchServiceClient(conn)
	response, err := client.Close(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}
