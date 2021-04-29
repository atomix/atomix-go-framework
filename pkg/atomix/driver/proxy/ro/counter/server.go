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

package counter

import (
	"context"
	counterapi "github.com/atomix/atomix-api/go/atomix/primitive/counter"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
)

var log = logging.GetLogger("atomix", "counter")

// NewProxyServer creates a new read-only counter server
func NewProxyServer(s counterapi.CounterServiceServer) counterapi.CounterServiceServer {
	return &ProxyServer{
		server: s,
	}
}

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
