// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package _map

import (
	"context"
	mapapi "github.com/atomix/atomix-api/go/atomix/primitive/map"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
)

// NewProxyServer creates a new read-only map server
func NewProxyServer(s mapapi.MapServiceServer) mapapi.MapServiceServer {
	return &ProxyServer{
		server: s,
	}
}

// ProxyServer is a read-only map primitive server
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
