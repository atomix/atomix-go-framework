package lock

import (
	"context"
	lock "github.com/atomix/api/go/atomix/primitive/lock"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"google.golang.org/grpc"
	"sync"
)

const Type = "Lock"

func RegisterService(node proxy.Node) {
	node.Services().RegisterService(func(s *grpc.Server) {
		server := &Server{
			node:      node,
			instances: make(map[string]lock.LockServiceServer),
			log:       logging.GetLogger("atomix", "lock"),
		}
		lock.RegisterLockServiceServer(s, server)
	})
}

type Server struct {
	node      proxy.Node
	instances map[string]lock.LockServiceServer
	mu        sync.RWMutex
	log       logging.Logger
}

func (s *Server) getInstance(name string) (lock.LockServiceServer, error) {
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

	primitiveMeta, err := s.node.Primitives().GetPrimitive(name)
	if err != nil {
		return nil, err
	}

	proxy, err := primitiveType.NewProxy()
	if err != nil {
		return nil, err
	}
	instance = proxy.(lock.LockServiceServer)

	if primitiveMeta.Cached {
		cached, err := primitiveType.NewCacheDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = cached.(lock.LockServiceServer)
	}

	if primitiveMeta.ReadOnly {
		readOnly, err := primitiveType.NewReadOnlyDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = readOnly.(lock.LockServiceServer)
	}

	s.instances[name] = instance
	return instance, nil
}

func (s *Server) Lock(ctx context.Context, request *lock.LockRequest) (*lock.LockResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("LockRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Lock(ctx, request)
}

func (s *Server) Unlock(ctx context.Context, request *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("UnlockRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Unlock(ctx, request)
}

func (s *Server) GetLock(ctx context.Context, request *lock.GetLockRequest) (*lock.GetLockResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("GetLockRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.GetLock(ctx, request)
}
