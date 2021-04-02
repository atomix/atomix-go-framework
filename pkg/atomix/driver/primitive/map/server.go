

package _map

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	_map "github.com/atomix/api/go/atomix/primitive/map"
)

// NewProxyServer creates a new ProxyServer
func NewProxyServer(registry *ProxyRegistry) _map.MapServiceServer {
	return &ProxyServer{
		registry: registry,
		log:      logging.GetLogger("atomix", "map"),
	}
}
type ProxyServer struct {
	registry *ProxyRegistry
	log      logging.Logger
}

func (s *ProxyServer) Size(ctx context.Context, request *_map.SizeRequest) (*_map.SizeResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("SizeRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Size(ctx, request)
}


func (s *ProxyServer) Put(ctx context.Context, request *_map.PutRequest) (*_map.PutResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("PutRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Put(ctx, request)
}


func (s *ProxyServer) Get(ctx context.Context, request *_map.GetRequest) (*_map.GetResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Get(ctx, request)
}


func (s *ProxyServer) Remove(ctx context.Context, request *_map.RemoveRequest) (*_map.RemoveResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("RemoveRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Remove(ctx, request)
}


func (s *ProxyServer) Clear(ctx context.Context, request *_map.ClearRequest) (*_map.ClearResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("ClearRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Clear(ctx, request)
}


func (s *ProxyServer) Events(request *_map.EventsRequest, srv _map.MapService_EventsServer) error {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return proxy.Events(request, srv)
}


func (s *ProxyServer) Entries(request *_map.EntriesRequest, srv _map.MapService_EntriesServer) error {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("EntriesRequest %+v failed: %v", request, err)
		return err
	}
	return proxy.Entries(request, srv)
}

