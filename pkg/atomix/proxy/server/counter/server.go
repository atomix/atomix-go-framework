package counter

import (
	"context"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"google.golang.org/grpc"
	"sync"
)

const Type = "Counter"

func RegisterService(node proxy.Node) {
	node.Services().RegisterService(func(s *grpc.Server) {
		server := &Server{
			node:      node,
			instances: make(map[string]counter.CounterServiceServer),
			log:       logging.GetLogger("atomix", "counter"),
		}
		counter.RegisterCounterServiceServer(s, server)
	})
}

type Server struct {
	node      proxy.Node
	instances map[string]counter.CounterServiceServer
	mu        sync.RWMutex
	log       logging.Logger
}

func (s *Server) getInstance(ctx context.Context, name string) (counter.CounterServiceServer, error) {
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
	instance = proxy.(counter.CounterServiceServer)

	if primitiveMeta.Cached {
		cached, err := primitiveType.NewCacheDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = cached.(counter.CounterServiceServer)
	}

	if primitiveMeta.ReadOnly {
		readOnly, err := primitiveType.NewReadOnlyDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = readOnly.(counter.CounterServiceServer)
	}

	s.instances[name] = instance
	return instance, nil
}

func (s *Server) Set(ctx context.Context, request *counter.SetRequest) (*counter.SetResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("SetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Set(ctx, request)
}

func (s *Server) Get(ctx context.Context, request *counter.GetRequest) (*counter.GetResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Get(ctx, request)
}

func (s *Server) Increment(ctx context.Context, request *counter.IncrementRequest) (*counter.IncrementResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("IncrementRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Increment(ctx, request)
}

func (s *Server) Decrement(ctx context.Context, request *counter.DecrementRequest) (*counter.DecrementResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("DecrementRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Decrement(ctx, request)
}
