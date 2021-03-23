

package _map

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	_map "github.com/atomix/api/go/atomix/primitive/map"
)

// NewMapProxyServer creates a new MapProxyServer
func NewMapProxyServer(registry *MapProxyRegistry) _map.MapServiceServer {
	return &MapProxyServer{
		registry: registry,
		log:      logging.GetLogger("atomix", "map"),
	}
}
type MapProxyServer struct {
	registry *MapProxyRegistry
	log      logging.Logger
}

func (s *MapProxyServer) Size(ctx context.Context, request *_map.SizeRequest) (*_map.SizeResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("SizeRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Size(ctx, request)
}


func (s *MapProxyServer) Put(ctx context.Context, request *_map.PutRequest) (*_map.PutResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("PutRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Put(ctx, request)
}


func (s *MapProxyServer) Get(ctx context.Context, request *_map.GetRequest) (*_map.GetResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Get(ctx, request)
}


func (s *MapProxyServer) Remove(ctx context.Context, request *_map.RemoveRequest) (*_map.RemoveResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("RemoveRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Remove(ctx, request)
}


func (s *MapProxyServer) Clear(ctx context.Context, request *_map.ClearRequest) (*_map.ClearResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("ClearRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Clear(ctx, request)
}


func (s *MapProxyServer) Events(request *_map.EventsRequest, srv _map.MapService_EventsServer) error {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return proxy.Events(request, srv)
}


func (s *MapProxyServer) Entries(request *_map.EntriesRequest, srv _map.MapService_EntriesServer) error {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("EntriesRequest %+v failed: %v", request, err)
		return err
	}
	return proxy.Entries(request, srv)
}

