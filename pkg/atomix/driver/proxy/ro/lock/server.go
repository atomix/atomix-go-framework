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

package lock

import (
	"context"
	lockapi "github.com/atomix/atomix-api/go/atomix/primitive/lock"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
)

var log = logging.GetLogger("atomix", "lock")

// NewProxyServer creates a new read-only lock server
func NewProxyServer(s lockapi.LockServiceServer) lockapi.LockServiceServer {
	return &ProxyServer{
		server: s,
	}
}

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
