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
	indexedmapapi "github.com/atomix/api/go/atomix/primitive/indexedmap"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
)

var log = logging.GetLogger("atomix", "indexedmap")

// NewReadOnlyIndexedMapServer creates a new read-only indexedmap server
func NewReadOnlyIndexedMapServer(s indexedmapapi.IndexedMapServiceServer) indexedmapapi.IndexedMapServiceServer {
	return &ReadOnlyIndexedMapServer{
		server: s,
	}
}

type ReadOnlyIndexedMapServer struct {
	server indexedmapapi.IndexedMapServiceServer
}

func (s *ReadOnlyIndexedMapServer) Size(ctx context.Context, request *indexedmapapi.SizeRequest) (*indexedmapapi.SizeResponse, error) {
	return s.server.Size(ctx, request)
}

func (s *ReadOnlyIndexedMapServer) Put(ctx context.Context, request *indexedmapapi.PutRequest) (*indexedmapapi.PutResponse, error) {
	return nil, errors.NewUnauthorized("Put operation is not permitted")
}

func (s *ReadOnlyIndexedMapServer) Get(ctx context.Context, request *indexedmapapi.GetRequest) (*indexedmapapi.GetResponse, error) {
	return s.server.Get(ctx, request)
}

func (s *ReadOnlyIndexedMapServer) FirstEntry(ctx context.Context, request *indexedmapapi.FirstEntryRequest) (*indexedmapapi.FirstEntryResponse, error) {
	return s.server.FirstEntry(ctx, request)
}

func (s *ReadOnlyIndexedMapServer) LastEntry(ctx context.Context, request *indexedmapapi.LastEntryRequest) (*indexedmapapi.LastEntryResponse, error) {
	return s.server.LastEntry(ctx, request)
}

func (s *ReadOnlyIndexedMapServer) PrevEntry(ctx context.Context, request *indexedmapapi.PrevEntryRequest) (*indexedmapapi.PrevEntryResponse, error) {
	return s.server.PrevEntry(ctx, request)
}

func (s *ReadOnlyIndexedMapServer) NextEntry(ctx context.Context, request *indexedmapapi.NextEntryRequest) (*indexedmapapi.NextEntryResponse, error) {
	return s.server.NextEntry(ctx, request)
}

func (s *ReadOnlyIndexedMapServer) Remove(ctx context.Context, request *indexedmapapi.RemoveRequest) (*indexedmapapi.RemoveResponse, error) {
	return nil, errors.NewUnauthorized("Remove operation is not permitted")
}

func (s *ReadOnlyIndexedMapServer) Clear(ctx context.Context, request *indexedmapapi.ClearRequest) (*indexedmapapi.ClearResponse, error) {
	return nil, errors.NewUnauthorized("Clear operation is not permitted")
}

func (s *ReadOnlyIndexedMapServer) Events(request *indexedmapapi.EventsRequest, server indexedmapapi.IndexedMapService_EventsServer) error {
	return s.server.Events(request, server)
}

func (s *ReadOnlyIndexedMapServer) Entries(request *indexedmapapi.EntriesRequest, server indexedmapapi.IndexedMapService_EntriesServer) error {
	return s.server.Entries(request, server)
}

var _ indexedmapapi.IndexedMapServiceServer = &ReadOnlyIndexedMapServer{}
