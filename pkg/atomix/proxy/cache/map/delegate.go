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
	"sync"
)

func NewServer(server _map.MapServiceServer) _map.MapServiceServer {
	return &DelegatingMap{
		server: server,
		maps:   make(map[string]_map.MapServiceServer),
	}
}

type DelegatingMap struct {
	server _map.MapServiceServer
	maps   map[string]_map.MapServiceServer
	mu     sync.RWMutex
}

func (s *DelegatingMap) getMap(name string) _map.MapServiceServer {
	s.mu.RLock()
	m, ok := s.maps[name]
	s.mu.RUnlock()
	if ok {
		return m
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok = s.maps[name]
	if !ok {
		m = newCachedMap(s.server)
		s.maps[name] = m
	}
	return m
}

func (s *DelegatingMap) Size(ctx context.Context, request *_map.SizeRequest) (*_map.SizeResponse, error) {
	return s.getMap(request.Headers.PrimitiveID).Size(ctx, request)
}

func (s *DelegatingMap) Put(ctx context.Context, request *_map.PutRequest) (*_map.PutResponse, error) {
	return s.getMap(request.Headers.PrimitiveID).Put(ctx, request)
}

func (s *DelegatingMap) Get(ctx context.Context, request *_map.GetRequest) (*_map.GetResponse, error) {
	return s.getMap(request.Headers.PrimitiveID).Get(ctx, request)
}

func (s *DelegatingMap) Remove(ctx context.Context, request *_map.RemoveRequest) (*_map.RemoveResponse, error) {
	return s.getMap(request.Headers.PrimitiveID).Remove(ctx, request)
}

func (s *DelegatingMap) Clear(ctx context.Context, request *_map.ClearRequest) (*_map.ClearResponse, error) {
	return s.getMap(request.Headers.PrimitiveID).Clear(ctx, request)
}

func (s *DelegatingMap) Events(request *_map.EventsRequest, server _map.MapService_EventsServer) error {
	return s.getMap(request.Headers.PrimitiveID).Events(request, server)
}

func (s *DelegatingMap) Entries(request *_map.EntriesRequest, server _map.MapService_EntriesServer) error {
	return s.getMap(request.Headers.PrimitiveID).Entries(request, server)
}
