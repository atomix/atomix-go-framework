// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
