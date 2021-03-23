package log

import (
	"context"
	log "github.com/atomix/api/go/atomix/primitive/log"
	"github.com/atomix/go-framework/pkg/atomix/logging"
)

// NewLogProxyServer creates a new LogProxyServer
func NewLogProxyServer(registry *LogProxyRegistry) log.LogServiceServer {
	return &LogProxyServer{
		registry: registry,
		log:      logging.GetLogger("atomix", "log"),
	}
}

type LogProxyServer struct {
	registry *LogProxyRegistry
	log      logging.Logger
}

func (s *LogProxyServer) Size(ctx context.Context, request *log.SizeRequest) (*log.SizeResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("SizeRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Size(ctx, request)
}

func (s *LogProxyServer) Append(ctx context.Context, request *log.AppendRequest) (*log.AppendResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("AppendRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Append(ctx, request)
}

func (s *LogProxyServer) Get(ctx context.Context, request *log.GetRequest) (*log.GetResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Get(ctx, request)
}

func (s *LogProxyServer) FirstEntry(ctx context.Context, request *log.FirstEntryRequest) (*log.FirstEntryResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("FirstEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.FirstEntry(ctx, request)
}

func (s *LogProxyServer) LastEntry(ctx context.Context, request *log.LastEntryRequest) (*log.LastEntryResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("LastEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.LastEntry(ctx, request)
}

func (s *LogProxyServer) PrevEntry(ctx context.Context, request *log.PrevEntryRequest) (*log.PrevEntryResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("PrevEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.PrevEntry(ctx, request)
}

func (s *LogProxyServer) NextEntry(ctx context.Context, request *log.NextEntryRequest) (*log.NextEntryResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("NextEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.NextEntry(ctx, request)
}

func (s *LogProxyServer) Remove(ctx context.Context, request *log.RemoveRequest) (*log.RemoveResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("RemoveRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Remove(ctx, request)
}

func (s *LogProxyServer) Clear(ctx context.Context, request *log.ClearRequest) (*log.ClearResponse, error) {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("ClearRequest %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.Clear(ctx, request)
}

func (s *LogProxyServer) Events(request *log.EventsRequest, srv log.LogService_EventsServer) error {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return proxy.Events(request, srv)
}

func (s *LogProxyServer) Entries(request *log.EntriesRequest, srv log.LogService_EntriesServer) error {
	proxy, err := s.registry.GetProxy(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EntriesRequest %+v failed: %v", request, err)
		return err
	}
	return proxy.Entries(request, srv)
}
