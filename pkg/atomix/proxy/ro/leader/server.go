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

package leader

import (
	"context"
	leaderapi "github.com/atomix/api/go/atomix/primitive/leader"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
)

var log = logging.GetLogger("atomix", "leader")

// NewReadOnlyLeaderLatchServer creates a new read-only leader server
func NewReadOnlyLeaderLatchServer(s leaderapi.LeaderLatchServiceServer) leaderapi.LeaderLatchServiceServer {
	return &ReadOnlyLeaderLatchServer{
		server: s,
	}
}

type ReadOnlyLeaderLatchServer struct {
	server leaderapi.LeaderLatchServiceServer
}

func (r *ReadOnlyLeaderLatchServer) Latch(ctx context.Context, request *leaderapi.LatchRequest) (*leaderapi.LatchResponse, error) {
	return nil, errors.NewUnauthorized("Latch operation is not permitted")
}

func (r *ReadOnlyLeaderLatchServer) Get(ctx context.Context, request *leaderapi.GetRequest) (*leaderapi.GetResponse, error) {
	return r.server.Get(ctx, request)
}

func (r *ReadOnlyLeaderLatchServer) Events(request *leaderapi.EventsRequest, server leaderapi.LeaderLatchService_EventsServer) error {
	return r.server.Events(request, server)
}

var _ leaderapi.LeaderLatchServiceServer = &ReadOnlyLeaderLatchServer{}
