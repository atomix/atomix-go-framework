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

package lock

import (
	"context"
	api "github.com/atomix/api/go/atomix/storage/lock"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
	"google.golang.org/grpc"
)

// RegisterPassthroughProxy registers the election primitive on the given node
func RegisterPassthroughProxy(node *passthrough.Node) {
	node.RegisterProxy(Type, func(server *grpc.Server, client *passthrough.Client) {
		api.RegisterLockServiceServer(server, &PassthroughProxy{
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

	client := api.NewLockServiceClient(conn)
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

	client := api.NewLockServiceClient(conn)
	response, err := client.Close(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}

// Lock acquires a lock
func (s *PassthroughProxy) Lock(ctx context.Context, request *api.LockRequest) (*api.LockResponse, error) {
	log.Debugf("Received LockRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLockServiceClient(conn)
	response, err := client.Lock(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending LockResponse %+v", response)
	return response, nil
}

// Unlock releases the lock
func (s *PassthroughProxy) Unlock(ctx context.Context, request *api.UnlockRequest) (*api.UnlockResponse, error) {
	log.Debugf("Received UnlockRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLockServiceClient(conn)
	response, err := client.Unlock(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending UnlockResponse %+v", response)
	return response, nil
}

// IsLocked checks whether the lock is held by any session
func (s *PassthroughProxy) IsLocked(ctx context.Context, request *api.IsLockedRequest) (*api.IsLockedResponse, error) {
	log.Debugf("Received IsLockedRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := api.NewLockServiceClient(conn)
	response, err := client.IsLocked(ctx, request)
	if err != nil {
		return nil, err
	}
	log.Debugf("Sending IsLockedResponse %+v", response)
	return response, nil
}
