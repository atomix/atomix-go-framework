

package counter

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
)

// NewProxyServer creates a new ProxyServer
func NewProxyServer(registry *ProxyRegistry) counter.CounterServiceServer {
	return &ProxyServer{
		registry: registry,
		log:      logging.GetLogger("atomix", "counter"),
	}
}
type ProxyServer struct {
	registry *ProxyRegistry
	log      logging.Logger
}

func (s *ProxyServer) Set(ctx context.Context, request *counter.SetRequest) (*counter.SetResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("SetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Set(ctx, request)
}


func (s *ProxyServer) Get(ctx context.Context, request *counter.GetRequest) (*counter.GetResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Get(ctx, request)
}


func (s *ProxyServer) Increment(ctx context.Context, request *counter.IncrementRequest) (*counter.IncrementResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("IncrementRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Increment(ctx, request)
}


func (s *ProxyServer) Decrement(ctx context.Context, request *counter.DecrementRequest) (*counter.DecrementResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("DecrementRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Decrement(ctx, request)
}

