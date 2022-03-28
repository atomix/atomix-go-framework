// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"context"
	counterapi "github.com/atomix/atomix-api/go/atomix/primitive/counter"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
)

// NewProxyServer creates a new read-only counter server
func NewProxyServer(s counterapi.CounterServiceServer) counterapi.CounterServiceServer {
	return &ProxyServer{
		server: s,
	}
}

// ProxyServer is a read-only counter primitive server
type ProxyServer struct {
	server counterapi.CounterServiceServer
}

func (s *ProxyServer) Get(ctx context.Context, request *counterapi.GetRequest) (*counterapi.GetResponse, error) {
	return s.server.Get(ctx, request)
}

func (s *ProxyServer) Set(ctx context.Context, request *counterapi.SetRequest) (*counterapi.SetResponse, error) {
	return nil, errors.NewUnauthorized("Set not authorized")
}

func (s *ProxyServer) Increment(ctx context.Context, request *counterapi.IncrementRequest) (*counterapi.IncrementResponse, error) {
	return nil, errors.NewUnauthorized("Increment not authorized")
}

func (s *ProxyServer) Decrement(ctx context.Context, request *counterapi.DecrementRequest) (*counterapi.DecrementResponse, error) {
	return nil, errors.NewUnauthorized("Decrement not authorized")
}

var _ counterapi.CounterServiceServer = &ProxyServer{}
