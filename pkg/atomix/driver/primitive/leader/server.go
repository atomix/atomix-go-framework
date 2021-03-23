

package leader

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	leader "github.com/atomix/api/go/atomix/primitive/leader"
)

// NewLeaderLatchProxyServer creates a new LeaderLatchProxyServer
func NewLeaderLatchProxyServer(registry *LeaderLatchProxyRegistry) leader.LeaderLatchServiceServer {
	return &LeaderLatchProxyServer{
		registry: registry,
		log:      logging.GetLogger("atomix", "leaderlatch"),
	}
}
type LeaderLatchProxyServer struct {
	registry *LeaderLatchProxyRegistry
	log      logging.Logger
}

func (s *LeaderLatchProxyServer) Latch(ctx context.Context, request *leader.LatchRequest) (*leader.LatchResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("LatchRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Latch(ctx, request)
}


func (s *LeaderLatchProxyServer) Get(ctx context.Context, request *leader.GetRequest) (*leader.GetResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Get(ctx, request)
}


func (s *LeaderLatchProxyServer) Events(request *leader.EventsRequest, srv leader.LeaderLatchService_EventsServer) error {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return proxy.Events(request, srv)
}

