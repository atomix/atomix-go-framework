// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	valueapi "github.com/atomix/atomix-api/go/atomix/primitive/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
)

// NewProxyServer creates a new read-only value server
func NewProxyServer(s valueapi.ValueServiceServer) valueapi.ValueServiceServer {
	return &ProxyServer{
		server: s,
	}
}

// ProxyServer is a read-only value primitive server
type ProxyServer struct {
	server valueapi.ValueServiceServer
}

func (s *ProxyServer) Set(ctx context.Context, request *valueapi.SetRequest) (*valueapi.SetResponse, error) {
	return nil, errors.NewUnauthorized("Set operation is not permitted")
}

func (s *ProxyServer) Get(ctx context.Context, request *valueapi.GetRequest) (*valueapi.GetResponse, error) {
	return s.server.Get(ctx, request)
}

func (s *ProxyServer) Events(request *valueapi.EventsRequest, server valueapi.ValueService_EventsServer) error {
	return s.server.Events(request, server)
}

var _ valueapi.ValueServiceServer = &ProxyServer{}
