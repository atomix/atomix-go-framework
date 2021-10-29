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

package indexedmap

import (
	"context"
	indexedmapapi "github.com/atomix/atomix-api/go/atomix/primitive/indexedmap"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/errors"
)

// NewProxyServer creates a new read-only indexedmap server
func NewProxyServer(s indexedmapapi.IndexedMapServiceServer) indexedmapapi.IndexedMapServiceServer {
	return &ProxyServer{
		server: s,
	}
}

// ProxyServer is a read-only indexed map primitive server
type ProxyServer struct {
	server indexedmapapi.IndexedMapServiceServer
}

func (s *ProxyServer) Size(ctx context.Context, request *indexedmapapi.SizeRequest) (*indexedmapapi.SizeResponse, error) {
	return s.server.Size(ctx, request)
}

func (s *ProxyServer) Put(ctx context.Context, request *indexedmapapi.PutRequest) (*indexedmapapi.PutResponse, error) {
	return nil, errors.NewUnauthorized("Put operation is not permitted")
}

func (s *ProxyServer) Get(ctx context.Context, request *indexedmapapi.GetRequest) (*indexedmapapi.GetResponse, error) {
	return s.server.Get(ctx, request)
}

func (s *ProxyServer) FirstEntry(ctx context.Context, request *indexedmapapi.FirstEntryRequest) (*indexedmapapi.FirstEntryResponse, error) {
	return s.server.FirstEntry(ctx, request)
}

func (s *ProxyServer) LastEntry(ctx context.Context, request *indexedmapapi.LastEntryRequest) (*indexedmapapi.LastEntryResponse, error) {
	return s.server.LastEntry(ctx, request)
}

func (s *ProxyServer) PrevEntry(ctx context.Context, request *indexedmapapi.PrevEntryRequest) (*indexedmapapi.PrevEntryResponse, error) {
	return s.server.PrevEntry(ctx, request)
}

func (s *ProxyServer) NextEntry(ctx context.Context, request *indexedmapapi.NextEntryRequest) (*indexedmapapi.NextEntryResponse, error) {
	return s.server.NextEntry(ctx, request)
}

func (s *ProxyServer) Remove(ctx context.Context, request *indexedmapapi.RemoveRequest) (*indexedmapapi.RemoveResponse, error) {
	return nil, errors.NewUnauthorized("Remove operation is not permitted")
}

func (s *ProxyServer) Clear(ctx context.Context, request *indexedmapapi.ClearRequest) (*indexedmapapi.ClearResponse, error) {
	return nil, errors.NewUnauthorized("Clear operation is not permitted")
}

func (s *ProxyServer) Events(request *indexedmapapi.EventsRequest, server indexedmapapi.IndexedMapService_EventsServer) error {
	return s.server.Events(request, server)
}

func (s *ProxyServer) Entries(request *indexedmapapi.EntriesRequest, server indexedmapapi.IndexedMapService_EntriesServer) error {
	return s.server.Entries(request, server)
}

var _ indexedmapapi.IndexedMapServiceServer = &ProxyServer{}
