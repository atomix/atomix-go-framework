

package indexedmap

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	indexedmap "github.com/atomix/api/go/atomix/primitive/indexedmap"
)

// NewIndexedMapProxyServer creates a new IndexedMapProxyServer
func NewIndexedMapProxyServer(registry *IndexedMapProxyRegistry) indexedmap.IndexedMapServiceServer {
	return &IndexedMapProxyServer{
		registry: registry,
		log:      logging.GetLogger("atomix", "indexedmap"),
	}
}
type IndexedMapProxyServer struct {
	registry *IndexedMapProxyRegistry
	log      logging.Logger
}

func (s *IndexedMapProxyServer) Size(ctx context.Context, request *indexedmap.SizeRequest) (*indexedmap.SizeResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("SizeRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Size(ctx, request)
}


func (s *IndexedMapProxyServer) Put(ctx context.Context, request *indexedmap.PutRequest) (*indexedmap.PutResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("PutRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Put(ctx, request)
}


func (s *IndexedMapProxyServer) Get(ctx context.Context, request *indexedmap.GetRequest) (*indexedmap.GetResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Get(ctx, request)
}


func (s *IndexedMapProxyServer) FirstEntry(ctx context.Context, request *indexedmap.FirstEntryRequest) (*indexedmap.FirstEntryResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("FirstEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.FirstEntry(ctx, request)
}


func (s *IndexedMapProxyServer) LastEntry(ctx context.Context, request *indexedmap.LastEntryRequest) (*indexedmap.LastEntryResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("LastEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.LastEntry(ctx, request)
}


func (s *IndexedMapProxyServer) PrevEntry(ctx context.Context, request *indexedmap.PrevEntryRequest) (*indexedmap.PrevEntryResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("PrevEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.PrevEntry(ctx, request)
}


func (s *IndexedMapProxyServer) NextEntry(ctx context.Context, request *indexedmap.NextEntryRequest) (*indexedmap.NextEntryResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("NextEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.NextEntry(ctx, request)
}


func (s *IndexedMapProxyServer) Remove(ctx context.Context, request *indexedmap.RemoveRequest) (*indexedmap.RemoveResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("RemoveRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Remove(ctx, request)
}


func (s *IndexedMapProxyServer) Clear(ctx context.Context, request *indexedmap.ClearRequest) (*indexedmap.ClearResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("ClearRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Clear(ctx, request)
}


func (s *IndexedMapProxyServer) Events(request *indexedmap.EventsRequest, srv indexedmap.IndexedMapService_EventsServer) error {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return proxy.Events(request, srv)
}


func (s *IndexedMapProxyServer) Entries(request *indexedmap.EntriesRequest, srv indexedmap.IndexedMapService_EntriesServer) error {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
	    s.log.Warnf("EntriesRequest %+v failed: %v", request, err)
		return err
	}
	return proxy.Entries(request, srv)
}

