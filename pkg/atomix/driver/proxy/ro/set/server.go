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
	setapi "github.com/atomix/atomix-api/go/atomix/primitive/set"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
)

// NewProxyServer creates a new read-only set server
func NewProxyServer(s setapi.SetServiceServer) setapi.SetServiceServer {
	return &ProxyServer{
		server: s,
	}
}

// ProxyServer is a read-only set primitive server
type ProxyServer struct {
	server setapi.SetServiceServer
}

func (s *ProxyServer) Size(ctx context.Context, request *setapi.SizeRequest) (*setapi.SizeResponse, error) {
	return s.server.Size(ctx, request)
}

func (s *ProxyServer) Contains(ctx context.Context, request *setapi.ContainsRequest) (*setapi.ContainsResponse, error) {
	return s.server.Contains(ctx, request)
}

func (s *ProxyServer) Add(ctx context.Context, request *setapi.AddRequest) (*setapi.AddResponse, error) {
	return nil, errors.NewUnauthorized("Add operation is not permitted")
}

func (s *ProxyServer) Remove(ctx context.Context, request *setapi.RemoveRequest) (*setapi.RemoveResponse, error) {
	return nil, errors.NewUnauthorized("Remove operation is not permitted")
}

func (s *ProxyServer) Clear(ctx context.Context, request *setapi.ClearRequest) (*setapi.ClearResponse, error) {
	return nil, errors.NewUnauthorized("Clear operation is not permitted")
}

func (s *ProxyServer) Events(request *setapi.EventsRequest, server setapi.SetService_EventsServer) error {
	return s.server.Events(request, server)
}

func (s *ProxyServer) Elements(request *setapi.ElementsRequest, server setapi.SetService_ElementsServer) error {
	return s.server.Elements(request, server)
}

var _ setapi.SetServiceServer = &ProxyServer{}
