

package lock

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	lock "github.com/atomix/api/go/atomix/primitive/lock"
)

// NewLockProxyServer creates a new LockProxyServer
func NewLockProxyServer(registry *LockProxyRegistry) lock.LockServiceServer {
	return &LockProxyServer{
		registry: registry,
		log:      logging.GetLogger("atomix", "lock"),
	}
}
type LockProxyServer struct {
	registry *LockProxyRegistry
	log      logging.Logger
}

func (s *LockProxyServer) Lock(ctx context.Context, request *lock.LockRequest) (*lock.LockResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("LockRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Lock(ctx, request)
}


func (s *LockProxyServer) Unlock(ctx context.Context, request *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("UnlockRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Unlock(ctx, request)
}


func (s *LockProxyServer) GetLock(ctx context.Context, request *lock.GetLockRequest) (*lock.GetLockResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("GetLockRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.GetLock(ctx, request)
}

