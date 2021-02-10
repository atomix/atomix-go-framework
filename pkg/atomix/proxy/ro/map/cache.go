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
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
)

const Type = "Map"

// RegisterReadOnlyMapDecorator registers the read-only map decorator on the given server
func RegisterReadOnlyMapDecorator(node proxy.Node) {
	node.PrimitiveTypes().RegisterReadOnlyDecoratorFunc(Type, func(s interface{}) interface{} {
		return &ReadOnlyMap{
			server: s.(_map.MapServiceServer),
		}
	})
}

var log = logging.GetLogger("atomix", "map")

type ReadOnlyMap struct {
	server _map.MapServiceServer
}

func (s *ReadOnlyMap) Size(ctx context.Context, request *_map.SizeRequest) (*_map.SizeResponse, error) {
	return s.server.Size(ctx, request)
}

func (s *ReadOnlyMap) Put(ctx context.Context, request *_map.PutRequest) (*_map.PutResponse, error) {
	return nil, errors.NewUnauthorized("Put not authorized")
}

func (s *ReadOnlyMap) Get(ctx context.Context, request *_map.GetRequest) (*_map.GetResponse, error) {
	return s.server.Get(ctx, request)
}

func (s *ReadOnlyMap) Remove(ctx context.Context, request *_map.RemoveRequest) (*_map.RemoveResponse, error) {
	return nil, errors.NewUnauthorized("Remove not authorized")
}

func (s *ReadOnlyMap) Clear(ctx context.Context, request *_map.ClearRequest) (*_map.ClearResponse, error) {
	return nil, errors.NewUnauthorized("Clear not authorized")
}

func (s *ReadOnlyMap) Events(request *_map.EventsRequest, server _map.MapService_EventsServer) error {
	return s.server.Events(request, server)
}

func (s *ReadOnlyMap) Entries(request *_map.EntriesRequest, server _map.MapService_EntriesServer) error {
	return s.server.Entries(request, server)
}
