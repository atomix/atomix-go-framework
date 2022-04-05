// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	"context"
	lockapi "github.com/atomix/atomix-api/go/atomix/primitive/lock"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
)

// NewProxyServer creates a new read-only lock server
func NewProxyServer(s lockapi.LockServiceServer) lockapi.LockServiceServer {
	return &ProxyServer{
		server: s,
	}
}

// ProxyServer is a read-only lock primitive server
type ProxyServer struct {
	server lockapi.LockServiceServer
}

func (s *ProxyServer) Lock(ctx context.Context, request *lockapi.LockRequest) (*lockapi.LockResponse, error) {
	return nil, errors.NewUnauthorized("Lock operation is not permitted")
}

func (s *ProxyServer) Unlock(ctx context.Context, request *lockapi.UnlockRequest) (*lockapi.UnlockResponse, error) {
	return nil, errors.NewUnauthorized("Unlock operation is not permitted")
}

func (s *ProxyServer) GetLock(ctx context.Context, request *lockapi.GetLockRequest) (*lockapi.GetLockResponse, error) {
	return s.server.GetLock(ctx, request)
}

var _ lockapi.LockServiceServer = &ProxyServer{}
