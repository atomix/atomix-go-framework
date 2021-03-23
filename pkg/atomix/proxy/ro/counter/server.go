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
	counterapi "github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
)

var log = logging.GetLogger("atomix", "counter")

// NewReadOnlyCounterServer creates a new read-only counter server
func NewReadOnlyCounterServer(s counterapi.CounterServiceServer) counterapi.CounterServiceServer {
	return &ReadOnlyCounterServer{
		server: s,
	}
}

type ReadOnlyCounterServer struct {
	server counterapi.CounterServiceServer
}

func (s *ReadOnlyCounterServer) Get(ctx context.Context, request *counterapi.GetRequest) (*counterapi.GetResponse, error) {
	return s.server.Get(ctx, request)
}

func (s *ReadOnlyCounterServer) Set(ctx context.Context, request *counterapi.SetRequest) (*counterapi.SetResponse, error) {
	return nil, errors.NewUnauthorized("Set not authorized")
}

func (s *ReadOnlyCounterServer) Increment(ctx context.Context, request *counterapi.IncrementRequest) (*counterapi.IncrementResponse, error) {
	return nil, errors.NewUnauthorized("Increment not authorized")
}

func (s *ReadOnlyCounterServer) Decrement(ctx context.Context, request *counterapi.DecrementRequest) (*counterapi.DecrementResponse, error) {
	return nil, errors.NewUnauthorized("Decrement not authorized")
}

var _ counterapi.CounterServiceServer = &ReadOnlyCounterServer{}
