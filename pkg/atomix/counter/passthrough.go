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

package counter

import (
	"context"
	api "github.com/atomix/api/go/atomix/storage/counter"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
	"google.golang.org/grpc"
)

// RegisterPassthroughProxy registers the primitive server on the given node
func RegisterPassthroughProxy(node *passthrough.Node) {
	node.RegisterProxy(Type, func(server *grpc.Server, client *passthrough.Client) {
		api.RegisterCounterServiceServer(server, &PassthroughProxy{
			Proxy: passthrough.NewProxy(client),
		})
	})
}

// PassthroughProxy is an implementation of CounterServiceServer for the counter primitive
type PassthroughProxy struct {
	*passthrough.Proxy
}

// Create opens a new session
func (s *PassthroughProxy) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	log.Debugf("Received CreateRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewCounterServiceClient(conn)
	response, err := client.Create(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending CreateResponse %+v", response)
	return response, nil
}

// Set sets the current value of the counter
func (s *PassthroughProxy) Set(ctx context.Context, request *api.SetRequest) (*api.SetResponse, error) {
	log.Debugf("Received SetRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewCounterServiceClient(conn)
	response, err := client.Set(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

// Get gets the current value of the counter
func (s *PassthroughProxy) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Debugf("Received GetRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewCounterServiceClient(conn)
	response, err := client.Get(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

// Increment increments the value of the counter by a delta
func (s *PassthroughProxy) Increment(ctx context.Context, request *api.IncrementRequest) (*api.IncrementResponse, error) {
	log.Debugf("Received IncrementRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewCounterServiceClient(conn)
	response, err := client.Increment(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending IncrementResponse %+v", response)
	return response, nil
}

// Decrement decrements the value of the counter by a delta
func (s *PassthroughProxy) Decrement(ctx context.Context, request *api.DecrementRequest) (*api.DecrementResponse, error) {
	log.Debugf("Received DecrementRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewCounterServiceClient(conn)
	response, err := client.Decrement(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending DecrementResponse %+v", response)
	return response, nil
}

// CheckAndSet updates the value of the counter conditionally
func (s *PassthroughProxy) CheckAndSet(ctx context.Context, request *api.CheckAndSetRequest) (*api.CheckAndSetResponse, error) {
	log.Debugf("Received CheckAndSetRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewCounterServiceClient(conn)
	response, err := client.CheckAndSet(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending CheckAndSetResponse %+v", response)
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

	client := api.NewCounterServiceClient(conn)
	response, err := client.Close(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}
