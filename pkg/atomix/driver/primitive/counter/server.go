package counter

import (
	"context"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/logging"
)

// NewCounterProxyServer creates a new CounterProxyServer
func NewCounterProxyServer(registry *CounterProxyRegistry) counter.CounterServiceServer {
	return &CounterProxyServer{
		registry: registry,
		log:      logging.GetLogger("atomix", "counter"),
	}
}

type CounterProxyServer struct {
	registry *CounterProxyRegistry
	log      logging.Logger
}

func (s *CounterProxyServer) Set(ctx context.Context, request *counter.SetRequest) (*counter.SetResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("SetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Set(ctx, request)
}

func (s *CounterProxyServer) Get(ctx context.Context, request *counter.GetRequest) (*counter.GetResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Get(ctx, request)
}

func (s *CounterProxyServer) Increment(ctx context.Context, request *counter.IncrementRequest) (*counter.IncrementResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("IncrementRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Increment(ctx, request)
}

func (s *CounterProxyServer) Decrement(ctx context.Context, request *counter.DecrementRequest) (*counter.DecrementResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("DecrementRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Decrement(ctx, request)
}
