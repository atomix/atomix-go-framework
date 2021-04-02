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
	listapi "github.com/atomix/api/go/atomix/primitive/list"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
)

var log = logging.GetLogger("atomix", "list")

// NewReadOnlyListServer creates a new read-only list server
func NewReadOnlyListServer(s listapi.ListServiceServer) listapi.ListServiceServer {
	return &ReadOnlyListServer{
		server: s,
	}
}

type ReadOnlyListServer struct {
	server listapi.ListServiceServer
}

func (s *ReadOnlyListServer) Size(ctx context.Context, request *listapi.SizeRequest) (*listapi.SizeResponse, error) {
	return s.server.Size(ctx, request)
}

func (s *ReadOnlyListServer) Append(ctx context.Context, request *listapi.AppendRequest) (*listapi.AppendResponse, error) {
	return nil, errors.NewUnauthorized("Append operation is not permitted")
}

func (s *ReadOnlyListServer) Insert(ctx context.Context, request *listapi.InsertRequest) (*listapi.InsertResponse, error) {
	return nil, errors.NewUnauthorized("Insert operation is not permitted")
}

func (s *ReadOnlyListServer) Get(ctx context.Context, request *listapi.GetRequest) (*listapi.GetResponse, error) {
	return s.server.Get(ctx, request)
}

func (s *ReadOnlyListServer) Set(ctx context.Context, request *listapi.SetRequest) (*listapi.SetResponse, error) {
	return nil, errors.NewUnauthorized("Set operation is not permitted")
}

func (s *ReadOnlyListServer) Remove(ctx context.Context, request *listapi.RemoveRequest) (*listapi.RemoveResponse, error) {
	return nil, errors.NewUnauthorized("Remove operation is not permitted")
}

func (s *ReadOnlyListServer) Clear(ctx context.Context, request *listapi.ClearRequest) (*listapi.ClearResponse, error) {
	return nil, errors.NewUnauthorized("Clear operation is not permitted")
}

func (s *ReadOnlyListServer) Events(request *listapi.EventsRequest, server listapi.ListService_EventsServer) error {
	return s.server.Events(request, server)
}

func (s *ReadOnlyListServer) Elements(request *listapi.ElementsRequest, server listapi.ListService_ElementsServer) error {
	return s.server.Elements(request, server)
}

var _ listapi.ListServiceServer = &ReadOnlyListServer{}
