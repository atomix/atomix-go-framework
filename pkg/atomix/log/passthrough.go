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

package log

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
	"google.golang.org/grpc"
	"io"

	api "github.com/atomix/api/go/atomix/storage/log"
)

// RegisterPassthroughProxy registers the election primitive on the given node
func RegisterPassthroughProxy(node *passthrough.Node) {
	node.RegisterProxy(Type, func(server *grpc.Server, client *passthrough.Client) {
		api.RegisterLogServiceServer(server, &PassthroughProxy{
			Proxy: passthrough.NewProxy(client),
		})
	})
}

// PassthroughProxy is an implementation of LogServiceServer for the log primitive
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

	client := api.NewLogServiceClient(conn)
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

	client := api.NewLogServiceClient(conn)
	response, err := client.Close(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}

// Size gets the number of entries in the log
func (s *PassthroughProxy) Size(ctx context.Context, request *api.SizeRequest) (*api.SizeResponse, error) {
	log.Debugf("Received SizeRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLogServiceClient(conn)
	response, err := client.Size(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

// Exists checks whether the log contains an index
func (s *PassthroughProxy) Exists(ctx context.Context, request *api.ExistsRequest) (*api.ExistsResponse, error) {
	log.Debugf("Received ExistsRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLogServiceClient(conn)
	response, err := client.Exists(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending ExistsResponse %+v", response)
	return response, nil
}

// Append appends a value to the end of the log
func (s *PassthroughProxy) Append(ctx context.Context, request *api.AppendRequest) (*api.AppendResponse, error) {
	log.Debugf("Received PutRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLogServiceClient(conn)
	response, err := client.Append(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending PutResponse %+v", response)
	return response, nil
}

// Get gets the value of an index
func (s *PassthroughProxy) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Debugf("Received GetRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLogServiceClient(conn)
	response, err := client.Get(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

// FirstEntry gets the first entry in the log
func (s *PassthroughProxy) FirstEntry(ctx context.Context, request *api.FirstEntryRequest) (*api.FirstEntryResponse, error) {
	log.Debugf("Received FirstEntryRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLogServiceClient(conn)
	response, err := client.FirstEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending FirstEntryResponse %+v", response)
	return response, nil
}

// LastEntry gets the last entry in the log
func (s *PassthroughProxy) LastEntry(ctx context.Context, request *api.LastEntryRequest) (*api.LastEntryResponse, error) {
	log.Debugf("Received LastEntryRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLogServiceClient(conn)
	response, err := client.LastEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending LastEntryResponse %+v", response)
	return response, nil
}

// PrevEntry gets the previous entry in the log
func (s *PassthroughProxy) PrevEntry(ctx context.Context, request *api.PrevEntryRequest) (*api.PrevEntryResponse, error) {
	log.Debugf("Received PrevEntryRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLogServiceClient(conn)
	response, err := client.PrevEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending PrevEntryResponse %+v", response)
	return response, nil
}

// NextEntry gets the next entry in the log
func (s *PassthroughProxy) NextEntry(ctx context.Context, request *api.NextEntryRequest) (*api.NextEntryResponse, error) {
	log.Debugf("Received NextEntryRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLogServiceClient(conn)
	response, err := client.NextEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending NextEntryResponse %+v", response)
	return response, nil
}

// Remove removes a key from the log
func (s *PassthroughProxy) Remove(ctx context.Context, request *api.RemoveRequest) (*api.RemoveResponse, error) {
	log.Debugf("Received RemoveRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLogServiceClient(conn)
	response, err := client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending RemoveRequest %+v", response)
	return response, nil
}

// Events listens for log change events
func (s *PassthroughProxy) Events(request *api.EventRequest, srv api.LogService_EventsServer) error {
	log.Debugf("Received EventRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return err
	}

	client := api.NewLogServiceClient(conn)
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

// Entries lists all entries currently in the log
func (s *PassthroughProxy) Entries(request *api.EntriesRequest, srv api.LogService_EntriesServer) error {
	log.Debugf("Received EntriesRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return err
	}

	client := api.NewLogServiceClient(conn)
	stream, err := client.Entries(srv.Context(), request)
	if err != nil {
		return err
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			log.Debugf("Finished EntriesRequest %+v", request)
			return nil
		} else if err != nil {
			return err
		}
		log.Debugf("Sending EntriesResponse %+v", response)
		if err := srv.Send(response); err != nil {
			return err
		}
	}
}

// Clear removes all keys from the log
func (s *PassthroughProxy) Clear(ctx context.Context, request *api.ClearRequest) (*api.ClearResponse, error) {
	log.Debugf("Received ClearRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLogServiceClient(conn)
	response, err := client.Clear(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}
