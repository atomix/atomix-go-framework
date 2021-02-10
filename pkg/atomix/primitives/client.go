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

package primitives

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"google.golang.org/grpc"
	"sync"
)

// NewRegistryClient creates a new primitives registry client
func NewRegistryClient(coordinator *cluster.Replica) *RegistryClient {
	return &RegistryClient{
		coordinator: coordinator,
	}
}

// RegistryClient is a primitives registry client
type RegistryClient struct {
	coordinator *cluster.Replica
	conn        *grpc.ClientConn
	mu          sync.RWMutex
}

func (r *RegistryClient) connect(ctx context.Context) (*grpc.ClientConn, error) {
	r.mu.RLock()
	conn := r.conn
	r.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	conn = r.conn
	if conn != nil {
		return conn, nil
	}

	conn, err := r.coordinator.Connect(ctx, cluster.WithDialOption(grpc.WithInsecure()))
	if err != nil {
		return nil, err
	}
	r.conn = conn
	return conn, nil
}

func (r *RegistryClient) GetPrimitive(ctx context.Context, name string) (*primitiveapi.PrimitiveMeta, error) {
	conn, err := r.connect(ctx)
	if err != nil {
		return nil, err
	}
	client := primitiveapi.NewPrimitiveRegistryServiceClient(conn)
	request := &primitiveapi.GetPrimitiveRequest{
		Name: name,
	}
	response, err := client.GetPrimitive(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Primitive, nil
}

func (r *RegistryClient) ListPrimitives(ctx context.Context) ([]primitiveapi.PrimitiveMeta, error) {
	conn, err := r.connect(ctx)
	if err != nil {
		return nil, err
	}
	client := primitiveapi.NewPrimitiveRegistryServiceClient(conn)
	request := &primitiveapi.ListPrimitivesRequest{}
	response, err := client.ListPrimitives(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return response.Primitives, nil
}
