package election

import (
	"context"
	election "github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"google.golang.org/grpc"
	"sync"
)

const Type = "Election"

func RegisterService(node proxy.Node) {
	node.Services().RegisterService(func(s *grpc.Server) {
		server := &Server{
			node:      node,
			instances: make(map[string]election.LeaderElectionServiceServer),
			log:       logging.GetLogger("atomix", "election"),
		}
		election.RegisterLeaderElectionServiceServer(s, server)
	})
}

type Server struct {
	node      proxy.Node
	instances map[string]election.LeaderElectionServiceServer
	mu        sync.RWMutex
	log       logging.Logger
}

func (s *Server) getInstance(ctx context.Context, name string) (election.LeaderElectionServiceServer, error) {
	s.mu.RLock()
	instance, ok := s.instances[name]
	s.mu.RUnlock()
	if ok {
		return instance, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	instance, ok = s.instances[name]
	if ok {
		return instance, nil
	}

	primitiveType, err := s.node.PrimitiveTypes().GetPrimitiveType(Type)
	if err != nil {
		return nil, err
	}

	primitiveMeta, err := s.node.Primitives().GetPrimitive(ctx, name)
	if err != nil {
		return nil, err
	}

	proxy, err := primitiveType.NewProxy()
	if err != nil {
		return nil, err
	}
	instance = proxy.(election.LeaderElectionServiceServer)

	if primitiveMeta.Cached {
		cached, err := primitiveType.NewCacheDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = cached.(election.LeaderElectionServiceServer)
	}

	if primitiveMeta.ReadOnly {
		readOnly, err := primitiveType.NewReadOnlyDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = readOnly.(election.LeaderElectionServiceServer)
	}

	s.instances[name] = instance
	return instance, nil
}

func (s *Server) Enter(ctx context.Context, request *election.EnterRequest) (*election.EnterResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EnterRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Enter(ctx, request)
}

func (s *Server) Withdraw(ctx context.Context, request *election.WithdrawRequest) (*election.WithdrawResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("WithdrawRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Withdraw(ctx, request)
}

func (s *Server) Anoint(ctx context.Context, request *election.AnointRequest) (*election.AnointResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("AnointRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Anoint(ctx, request)
}

func (s *Server) Promote(ctx context.Context, request *election.PromoteRequest) (*election.PromoteResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("PromoteRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Promote(ctx, request)
}

func (s *Server) Evict(ctx context.Context, request *election.EvictRequest) (*election.EvictResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EvictRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Evict(ctx, request)
}

func (s *Server) GetTerm(ctx context.Context, request *election.GetTermRequest) (*election.GetTermResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("GetTermRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.GetTerm(ctx, request)
}

func (s *Server) Events(request *election.EventsRequest, srv election.LeaderElectionService_EventsServer) error {
	instance, err := s.getInstance(srv.Context(), request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return instance.Events(request, srv)
}
