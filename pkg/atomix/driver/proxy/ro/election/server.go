// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"context"
	electionapi "github.com/atomix/atomix-api/go/atomix/primitive/election"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
)

// NewProxyServer creates a new read-only election server
func NewProxyServer(s electionapi.LeaderElectionServiceServer) electionapi.LeaderElectionServiceServer {
	return &ProxyServer{
		server: s,
	}
}

// ProxyServer is a read-only election primitive server
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
