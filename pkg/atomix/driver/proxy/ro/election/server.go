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

package election

import (
	"context"
	electionapi "github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
)

var log = logging.GetLogger("atomix", "election")

// NewProxyServer creates a new read-only election server
func NewProxyServer(s electionapi.LeaderElectionServiceServer) electionapi.LeaderElectionServiceServer {
	return &ProxyServer{
		server: s,
	}
}

type ProxyServer struct {
	server electionapi.LeaderElectionServiceServer
}

func (s *ProxyServer) Enter(ctx context.Context, request *electionapi.EnterRequest) (*electionapi.EnterResponse, error) {
	return nil, errors.NewUnauthorized("Enter operation is not permitted")
}

func (s *ProxyServer) Withdraw(ctx context.Context, request *electionapi.WithdrawRequest) (*electionapi.WithdrawResponse, error) {
	return nil, errors.NewUnauthorized("Withdraw operation is not permitted")
}

func (s *ProxyServer) Anoint(ctx context.Context, request *electionapi.AnointRequest) (*electionapi.AnointResponse, error) {
	return nil, errors.NewUnauthorized("Anoint operation is not permitted")
}

func (s *ProxyServer) Promote(ctx context.Context, request *electionapi.PromoteRequest) (*electionapi.PromoteResponse, error) {
	return nil, errors.NewUnauthorized("Promote operation is not permitted")
}

func (s *ProxyServer) Evict(ctx context.Context, request *electionapi.EvictRequest) (*electionapi.EvictResponse, error) {
	return nil, errors.NewUnauthorized("Evict operation is not permitted")
}

func (s *ProxyServer) GetTerm(ctx context.Context, request *electionapi.GetTermRequest) (*electionapi.GetTermResponse, error) {
	return s.server.GetTerm(ctx, request)
}

func (s *ProxyServer) Events(request *electionapi.EventsRequest, server electionapi.LeaderElectionService_EventsServer) error {
	return s.server.Events(request, server)
}

var _ electionapi.LeaderElectionServiceServer = &ProxyServer{}
