// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"context"
	indexedmapapi "github.com/atomix/atomix-api/go/atomix/primitive/indexedmap"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
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
