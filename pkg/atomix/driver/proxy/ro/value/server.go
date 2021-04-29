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

package value

import (
	"context"
	valueapi "github.com/atomix/atomix-api/go/atomix/primitive/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
)

var log = logging.GetLogger("atomix", "value")

// NewProxyServer creates a new read-only value server
func NewProxyServer(s valueapi.ValueServiceServer) valueapi.ValueServiceServer {
	return &ProxyServer{
		server: s,
	}
}

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
