package lock

import (
	"context"
	lock "github.com/atomix/api/go/atomix/primitive/lock"
	"github.com/atomix/go-framework/pkg/atomix/logging"
)

// NewProxyServer creates a new ProxyServer
func NewProxyServer(registry *ProxyRegistry) lock.LockServiceServer {
	return &ProxyServer{
		registry: registry,
		log:      logging.GetLogger("atomix", "lock"),
	}
}

type ProxyServer struct {
	registry *ProxyRegistry
	log      logging.Logger
}

func (s *ProxyServer) Lock(ctx context.Context, request *lock.LockRequest) (*lock.LockResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("LockRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Lock(ctx, request)
}

func (s *ProxyServer) Unlock(ctx context.Context, request *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("UnlockRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Unlock(ctx, request)
}

func (s *ProxyServer) GetLock(ctx context.Context, request *lock.GetLockRequest) (*lock.GetLockResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("GetLockRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.GetLock(ctx, request)
}
