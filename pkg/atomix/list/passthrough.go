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

package list

import (
	"context"
	api "github.com/atomix/api/go/atomix/storage/list"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
	"google.golang.org/grpc"
	"io"
)

// RegisterPassthroughProxy registers the election primitive on the given node
func RegisterPassthroughProxy(node *passthrough.Node) {
	node.RegisterProxy(Type, func(server *grpc.Server, client *passthrough.Client) {
		api.RegisterListServiceServer(server, &PassthroughProxy{
			Proxy: passthrough.NewProxy(client),
		})
	})
}

// PassthroughProxy is an implementation of MapServiceServer for the map primitive
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

	client := api.NewListServiceClient(conn)
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

	client := api.NewListServiceClient(conn)
	response, err := client.Close(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}

// Size gets the number of elements in the list
func (s *PassthroughProxy) Size(ctx context.Context, request *api.SizeRequest) (*api.SizeResponse, error) {
	log.Debugf("Received SizeRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewListServiceClient(conn)
	response, err := client.Size(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

// Contains checks whether the list contains a value
func (s *PassthroughProxy) Contains(ctx context.Context, request *api.ContainsRequest) (*api.ContainsResponse, error) {
	log.Debugf("Received ContainsRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewListServiceClient(conn)
	response, err := client.Contains(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending ContainsResponse %+v", response)
	return response, nil
}

// Append adds a value to the end of the list
func (s *PassthroughProxy) Append(ctx context.Context, request *api.AppendRequest) (*api.AppendResponse, error) {
	log.Debugf("Received AppendRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewListServiceClient(conn)
	response, err := client.Append(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending AppendResponse %+v", response)
	return response, nil
}

// Insert inserts a value at a specific index
func (s *PassthroughProxy) Insert(ctx context.Context, request *api.InsertRequest) (*api.InsertResponse, error) {
	log.Debugf("Received InsertRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewListServiceClient(conn)
	response, err := client.Insert(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending InsertResponse %+v", response)
	return response, nil
}

// Set sets the value at a specific index
func (s *PassthroughProxy) Set(ctx context.Context, request *api.SetRequest) (*api.SetResponse, error) {
	log.Debugf("Received SetRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewListServiceClient(conn)
	response, err := client.Set(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

// Get gets the value at a specific index
func (s *PassthroughProxy) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Debugf("Received GetRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewListServiceClient(conn)
	response, err := client.Get(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

// Remove removes an index from the list
func (s *PassthroughProxy) Remove(ctx context.Context, request *api.RemoveRequest) (*api.RemoveResponse, error) {
	log.Debugf("Received RemoveRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewListServiceClient(conn)
	response, err := client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

// Clear removes all indexes from the list
func (s *PassthroughProxy) Clear(ctx context.Context, request *api.ClearRequest) (*api.ClearResponse, error) {
	log.Debugf("Received ClearRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewListServiceClient(conn)
	response, err := client.Clear(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

// Events listens for list change events
func (s *PassthroughProxy) Events(request *api.EventRequest, srv api.ListService_EventsServer) error {
	log.Debugf("Received EventRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return err
	}

	client := api.NewListServiceClient(conn)
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

// Iterate lists all the value in the list
func (s *PassthroughProxy) Iterate(request *api.IterateRequest, srv api.ListService_IterateServer) error {
	log.Debugf("Received IterateRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return err
	}

	client := api.NewListServiceClient(conn)
	stream, err := client.Iterate(srv.Context(), request)
	if err != nil {
		return err
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			log.Debugf("Finished IterateRequest %+v", request)
			return nil
		} else if err != nil {
			return err
		}
		log.Debugf("Sending IterateResponse %+v", response)
		if err := srv.Send(response); err != nil {
			return err
		}
	}
}
