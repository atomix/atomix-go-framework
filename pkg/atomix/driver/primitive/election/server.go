package election

import (
	"context"
	election "github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/go-framework/pkg/atomix/logging"
)

// NewElectionProxyServer creates a new ElectionProxyServer
func NewElectionProxyServer(registry *ElectionProxyRegistry) election.LeaderElectionServiceServer {
	return &ElectionProxyServer{
		registry: registry,
		log:      logging.GetLogger("atomix", "election"),
	}
}

type ElectionProxyServer struct {
	registry *ElectionProxyRegistry
	log      logging.Logger
}

func (s *ElectionProxyServer) Enter(ctx context.Context, request *election.EnterRequest) (*election.EnterResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EnterRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Enter(ctx, request)
}

func (s *ElectionProxyServer) Withdraw(ctx context.Context, request *election.WithdrawRequest) (*election.WithdrawResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("WithdrawRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Withdraw(ctx, request)
}

func (s *ElectionProxyServer) Anoint(ctx context.Context, request *election.AnointRequest) (*election.AnointResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("AnointRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Anoint(ctx, request)
}

func (s *ElectionProxyServer) Promote(ctx context.Context, request *election.PromoteRequest) (*election.PromoteResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("PromoteRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Promote(ctx, request)
}

func (s *ElectionProxyServer) Evict(ctx context.Context, request *election.EvictRequest) (*election.EvictResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EvictRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Evict(ctx, request)
}

func (s *ElectionProxyServer) GetTerm(ctx context.Context, request *election.GetTermRequest) (*election.GetTermResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("GetTermRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.GetTerm(ctx, request)
}

func (s *ElectionProxyServer) Events(request *election.EventsRequest, srv election.LeaderElectionService_EventsServer) error {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return proxy.Events(request, srv)
}
