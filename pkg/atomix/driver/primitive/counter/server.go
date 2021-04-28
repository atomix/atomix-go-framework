package counter

import (
	"context"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/driver/env"
	"github.com/atomix/go-framework/pkg/atomix/logging"
)

// NewProxyServer creates a new ProxyServer
func NewProxyServer(registry *ProxyRegistry, env env.DriverEnv) counter.CounterServiceServer {
	return &ProxyServer{
		registry: registry,
		env:      env,
		log:      logging.GetLogger("atomix", "counter"),
	}
}

type ProxyServer struct {
	registry *ProxyRegistry
	env      env.DriverEnv
	log      logging.Logger
}

func (s *ProxyServer) Set(ctx context.Context, request *counter.SetRequest) (*counter.SetResponse, error) {
	if request.Headers.PrimitiveID.Namespace == "" {
		request.Headers.PrimitiveID.Namespace = s.env.Namespace
	}
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("SetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Set(ctx, request)
}

func (s *ProxyServer) Get(ctx context.Context, request *counter.GetRequest) (*counter.GetResponse, error) {
	if request.Headers.PrimitiveID.Namespace == "" {
		request.Headers.PrimitiveID.Namespace = s.env.Namespace
	}
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Get(ctx, request)
}

func (s *ProxyServer) Increment(ctx context.Context, request *counter.IncrementRequest) (*counter.IncrementResponse, error) {
	if request.Headers.PrimitiveID.Namespace == "" {
		request.Headers.PrimitiveID.Namespace = s.env.Namespace
	}
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("IncrementRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Increment(ctx, request)
}

func (s *ProxyServer) Decrement(ctx context.Context, request *counter.DecrementRequest) (*counter.DecrementResponse, error) {
	if request.Headers.PrimitiveID.Namespace == "" {
		request.Headers.PrimitiveID.Namespace = s.env.Namespace
	}
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("DecrementRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Decrement(ctx, request)
}
