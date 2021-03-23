

package value

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	value "github.com/atomix/api/go/atomix/primitive/value"
)

// NewValueProxyServer creates a new ValueProxyServer
func NewValueProxyServer(registry *ValueProxyRegistry) value.ValueServiceServer {
	return &ValueProxyServer{
		registry: registry,
		log:      logging.GetLogger("atomix", "value"),
	}
}
type ValueProxyServer struct {
	registry *ValueProxyRegistry
	log      logging.Logger
}

func (s *ValueProxyServer) Set(ctx context.Context, request *value.SetRequest) (*value.SetResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("SetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Set(ctx, request)
}


func (s *ValueProxyServer) Get(ctx context.Context, request *value.GetRequest) (*value.GetResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Get(ctx, request)
}


func (s *ValueProxyServer) Events(request *value.EventsRequest, srv value.ValueService_EventsServer) error {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return proxy.Events(request, srv)
}

