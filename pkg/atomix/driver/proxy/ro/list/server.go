// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package list

import (
	"context"
	listapi "github.com/atomix/atomix-api/go/atomix/primitive/list"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
)

// NewProxyServer creates a new read-only list server
func NewProxyServer(s listapi.ListServiceServer) listapi.ListServiceServer {
	return &ProxyServer{
		server: s,
	}
}

// ProxyServer is a read-only list primitive server
type ProxyServer struct {
	server listapi.ListServiceServer
}

func (s *ProxyServer) Size(ctx context.Context, request *listapi.SizeRequest) (*listapi.SizeResponse, error) {
	return s.server.Size(ctx, request)
}

func (s *ProxyServer) Append(ctx context.Context, request *listapi.AppendRequest) (*listapi.AppendResponse, error) {
	return nil, errors.NewUnauthorized("Append operation is not permitted")
}

func (s *ProxyServer) Insert(ctx context.Context, request *listapi.InsertRequest) (*listapi.InsertResponse, error) {
	return nil, errors.NewUnauthorized("Insert operation is not permitted")
}

func (s *ProxyServer) Get(ctx context.Context, request *listapi.GetRequest) (*listapi.GetResponse, error) {
	return s.server.Get(ctx, request)
}

func (s *ProxyServer) Set(ctx context.Context, request *listapi.SetRequest) (*listapi.SetResponse, error) {
	return nil, errors.NewUnauthorized("Set operation is not permitted")
}

func (s *ProxyServer) Remove(ctx context.Context, request *listapi.RemoveRequest) (*listapi.RemoveResponse, error) {
	return nil, errors.NewUnauthorized("Remove operation is not permitted")
}

func (s *ProxyServer) Clear(ctx context.Context, request *listapi.ClearRequest) (*listapi.ClearResponse, error) {
	return nil, errors.NewUnauthorized("Clear operation is not permitted")
}

func (s *ProxyServer) Events(request *listapi.EventsRequest, server listapi.ListService_EventsServer) error {
	return s.server.Events(request, server)
}

func (s *ProxyServer) Elements(request *listapi.ElementsRequest, server listapi.ListService_ElementsServer) error {
	return s.server.Elements(request, server)
}

var _ listapi.ListServiceServer = &ProxyServer{}
