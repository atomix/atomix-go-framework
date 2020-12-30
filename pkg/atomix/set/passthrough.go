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
	api "github.com/atomix/api/go/atomix/storage/set"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc"
	"io"
	"sync"
)

// RegisterPassthroughProxy registers the election primitive on the given node
func RegisterPassthroughProxy(node *passthrough.Node) {
	node.RegisterProxy(Type, func(server *grpc.Server, client *passthrough.Client) {
		api.RegisterSetServiceServer(server, &PassthroughProxy{
			Proxy: passthrough.NewProxy(client),
		})
	})
}

// PassthroughProxy is an implementation of SetServiceServer for the set primitive
type PassthroughProxy struct {
	*passthrough.Proxy
}

// Size gets the number of elements in the set
func (s *PassthroughProxy) Size(ctx context.Context, request *api.SizeRequest) (*api.SizeResponse, error) {
	log.Debugf("Received SizeRequest %+v", request)
	partitions := s.Partitions()
	results, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		conn, err := partitions[i].Connect()
		if err != nil {
			return nil, err
		}
		client := api.NewSetServiceClient(conn)
		response, err := client.Size(ctx, request)
		if err != nil {
			return nil, err
		}
		return response.Size_, nil
	})
	if err != nil {
		return nil, err
	}

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
func (s *PassthroughProxy) Contains(ctx context.Context, request *api.ContainsRequest) (*api.ContainsResponse, error) {
	log.Debugf("Received ContainsRequest %+v", request)
	partition := s.PartitionBy([]byte(request.Value))
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}
	client := api.NewSetServiceClient(conn)
	response, err := client.Contains(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending ContainsResponse %+v", response)
	return response, nil
}

// Add adds an element to the set
func (s *PassthroughProxy) Add(ctx context.Context, request *api.AddRequest) (*api.AddResponse, error) {
	log.Debugf("Received AddRequest %+v", request)
	partition := s.PartitionBy([]byte(request.Value))
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}
	client := api.NewSetServiceClient(conn)
	response, err := client.Add(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending AddResponse %+v", response)
	return response, nil
}

// Remove removes an element from the set
func (s *PassthroughProxy) Remove(ctx context.Context, request *api.RemoveRequest) (*api.RemoveResponse, error) {
	log.Debugf("Received RemoveRequest %+v", request)
	partition := s.PartitionBy([]byte(request.Value))
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}
	client := api.NewSetServiceClient(conn)
	response, err := client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

// Clear removes all elements from the set
func (s *PassthroughProxy) Clear(ctx context.Context, request *api.ClearRequest) (*api.ClearResponse, error) {
	log.Debugf("Received ClearRequest %+v", request)
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		conn, err := partitions[i].Connect()
		if err != nil {
			return err
		}
		client := api.NewSetServiceClient(conn)
		_, err = client.Clear(ctx, request)
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
func (s *PassthroughProxy) Events(request *api.EventRequest, srv api.SetService_EventsServer) error {
	log.Debugf("Received EventRequest %+v", request)
	partitions := s.Partitions()
	wg := &sync.WaitGroup{}
	ch := make(chan *api.EventResponse)
	err := async.IterAsync(len(partitions), func(i int) error {
		conn, err := partitions[i].Connect()
		if err != nil {
			return err
		}
		client := api.NewSetServiceClient(conn)
		stream, err := client.Events(srv.Context(), request)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			wg.Done()
			for {
				response, err := stream.Recv()
				if err == io.EOF {
					return
				} else if err != nil {
					log.Error(err)
				} else {
					ch <- response
				}
			}
		}()
		return nil
	})
	if err != nil {
		return err
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for response := range ch {
		log.Debugf("Sending EventResponse %+v", response)
		err := srv.Send(response)
		if err != nil {
			return err
		}
	}
	log.Debugf("Finished EventRequest %+v", request)
	return nil
}

// Iterate lists all elements currently in the set
func (s *PassthroughProxy) Iterate(request *api.IterateRequest, srv api.SetService_IterateServer) error {
	log.Debugf("Received IterateRequest %+v", request)
	partitions := s.Partitions()
	wg := &sync.WaitGroup{}
	ch := make(chan *api.IterateResponse)
	err := async.IterAsync(len(partitions), func(i int) error {
		conn, err := partitions[i].Connect()
		if err != nil {
			return err
		}
		client := api.NewSetServiceClient(conn)
		stream, err := client.Iterate(srv.Context(), request)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			wg.Done()
			for {
				response, err := stream.Recv()
				if err == io.EOF {
					return
				} else if err != nil {
					log.Error(err)
				} else {
					ch <- response
				}
			}
		}()
		return nil
	})
	if err != nil {
		return err
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for response := range ch {
		log.Debugf("Sending IterateResponse %+v", response)
		err := srv.Send(response)
		if err != nil {
			return err
		}
	}
	log.Debugf("Finished IterateRequest %+v", request)
	return nil
}

// Create opens a new session
func (s *PassthroughProxy) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	log.Debugf("Received CreateRequest %+v", request)
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		conn, err := partitions[i].Connect()
		if err != nil {
			return err
		}
		client := api.NewSetServiceClient(conn)
		_, err = client.Create(ctx, request)
		return err
	})
	if err != nil {
		return nil, err
	}
	response := &api.CreateResponse{}
	log.Debugf("Sending CreateResponse %+v", response)
	return response, nil
}

// Close closes a session
func (s *PassthroughProxy) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
	log.Debugf("Received CloseRequest %+v", request)
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		conn, err := partitions[i].Connect()
		if err != nil {
			return err
		}
		client := api.NewSetServiceClient(conn)
		_, err = client.Close(ctx, request)
		return err
	})
	if err != nil {
		return nil, err
	}
	response := &api.CloseResponse{}
	log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}
