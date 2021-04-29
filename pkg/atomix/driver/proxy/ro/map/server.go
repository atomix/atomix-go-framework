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

package _map

import (
	"context"
	mapapi "github.com/atomix/atomix-api/go/atomix/primitive/map"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
)

var log = logging.GetLogger("atomix", "map")

// NewProxyServer creates a new read-only map server
func NewProxyServer(s mapapi.MapServiceServer) mapapi.MapServiceServer {
	return &ProxyServer{
		server: s,
	}
}

type ProxyServer struct {
	server mapapi.MapServiceServer
}

func (s *ProxyServer) Size(ctx context.Context, request *mapapi.SizeRequest) (*mapapi.SizeResponse, error) {
	return s.server.Size(ctx, request)
}

func (s *ProxyServer) Put(ctx context.Context, request *mapapi.PutRequest) (*mapapi.PutResponse, error) {
	return nil, errors.NewUnauthorized("Put not authorized")
}

func (s *ProxyServer) Get(ctx context.Context, request *mapapi.GetRequest) (*mapapi.GetResponse, error) {
	return s.server.Get(ctx, request)
}

func (s *ProxyServer) Remove(ctx context.Context, request *mapapi.RemoveRequest) (*mapapi.RemoveResponse, error) {
	return nil, errors.NewUnauthorized("Remove not authorized")
}

func (s *ProxyServer) Clear(ctx context.Context, request *mapapi.ClearRequest) (*mapapi.ClearResponse, error) {
	return nil, errors.NewUnauthorized("Clear not authorized")
}

func (s *ProxyServer) Events(request *mapapi.EventsRequest, server mapapi.MapService_EventsServer) error {
	return s.server.Events(request, server)
}

func (s *ProxyServer) Entries(request *mapapi.EntriesRequest, server mapapi.MapService_EntriesServer) error {
	return s.server.Entries(request, server)
}

var _ mapapi.MapServiceServer = &ProxyServer{}
