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

package _map //nolint:golint

import (
	"context"
	api "github.com/atomix/api/go/atomix/storage/map"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc"
	"io"
	"sync"
)

// RegisterPassthroughProxy registers the election primitive on the given node
func RegisterPassthroughProxy(node *passthrough.Node) {
	node.RegisterProxy(Type, func(server *grpc.Server, client *passthrough.Client) {
		api.RegisterMapServiceServer(server, &PassthroughProxy{
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
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		conn, err := partitions[i].Connect()
		if err != nil {
			return err
		}
		client := api.NewMapServiceClient(conn)
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
		client := api.NewMapServiceClient(conn)
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

// Size gets the number of entries in the map
func (s *PassthroughProxy) Size(ctx context.Context, request *api.SizeRequest) (*api.SizeResponse, error) {
	log.Debugf("Received SizeRequest %+v", request)
	partitions := s.Partitions()
	results, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		conn, err := partitions[i].Connect()
		if err != nil {
			return nil, err
		}
		client := api.NewMapServiceClient(conn)
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

// Exists checks whether the map contains a key
func (s *PassthroughProxy) Exists(ctx context.Context, request *api.ExistsRequest) (*api.ExistsResponse, error) {
	log.Debugf("Received ExistsRequest %+v", request)
	partition := s.PartitionBy([]byte(request.Key))
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}
	client := api.NewMapServiceClient(conn)
	response, err := client.Exists(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending ExistsResponse %+v", response)
	return response, nil
}

// Put puts a key/value pair into the map
func (s *PassthroughProxy) Put(ctx context.Context, request *api.PutRequest) (*api.PutResponse, error) {
	log.Debugf("Received PutRequest %+v", request)
	partition := s.PartitionBy([]byte(request.Key))
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}
	client := api.NewMapServiceClient(conn)
	response, err := client.Put(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending PutResponse %+v", response)
	return response, nil
}

// Get gets the value of a key
func (s *PassthroughProxy) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Debugf("Received GetRequest %+v", request)
	partition := s.PartitionBy([]byte(request.Key))
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}
	client := api.NewMapServiceClient(conn)
	response, err := client.Get(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending GetRequest %+v", response)
	return response, nil
}

// Remove removes a key from the map
func (s *PassthroughProxy) Remove(ctx context.Context, request *api.RemoveRequest) (*api.RemoveResponse, error) {
	log.Debugf("Received RemoveRequest %+v", request)
	partition := s.PartitionBy([]byte(request.Key))
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}
	client := api.NewMapServiceClient(conn)
	response, err := client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending RemoveRequest %+v", response)
	return response, nil
}

// Clear removes all keys from the map
func (s *PassthroughProxy) Clear(ctx context.Context, request *api.ClearRequest) (*api.ClearResponse, error) {
	log.Debugf("Received ClearRequest %+v", request)
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		conn, err := partitions[i].Connect()
		if err != nil {
			return err
		}
		client := api.NewMapServiceClient(conn)
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

// Events listens for map change events
func (s *PassthroughProxy) Events(request *api.EventRequest, srv api.MapService_EventsServer) error {
	log.Debugf("Received EventRequest %+v", request)
	partitions := s.Partitions()
	wg := &sync.WaitGroup{}
	ch := make(chan *api.EventResponse)
	err := async.IterAsync(len(partitions), func(i int) error {
		conn, err := partitions[i].Connect()
		if err != nil {
			return err
		}
		client := api.NewMapServiceClient(conn)
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

// Entries lists all entries currently in the map
func (s *PassthroughProxy) Entries(request *api.EntriesRequest, srv api.MapService_EntriesServer) error {
	log.Debugf("Received EntriesRequest %+v", request)
	partitions := s.Partitions()
	wg := &sync.WaitGroup{}
	ch := make(chan *api.EntriesResponse)
	err := async.IterAsync(len(partitions), func(i int) error {
		conn, err := partitions[i].Connect()
		if err != nil {
			return err
		}
		client := api.NewMapServiceClient(conn)
		stream, err := client.Entries(srv.Context(), request)
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
		log.Debugf("Sending EntriesResponse %+v", response)
		err := srv.Send(response)
		if err != nil {
			return err
		}
	}
	log.Debugf("Finished EntriesRequest %+v", request)
	return nil
}
