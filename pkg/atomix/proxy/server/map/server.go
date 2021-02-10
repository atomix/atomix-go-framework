package _map

import (
	"context"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"google.golang.org/grpc"
	"sync"
)

const Type = "Map"

func RegisterService(node proxy.Node) {
	node.Services().RegisterService(func(s *grpc.Server) {
		server := &Server{
			node:      node,
			instances: make(map[string]_map.MapServiceServer),
			log:       logging.GetLogger("atomix", "map"),
		}
		_map.RegisterMapServiceServer(s, server)
	})
}

type Server struct {
	node      proxy.Node
	instances map[string]_map.MapServiceServer
	mu        sync.RWMutex
	log       logging.Logger
}

func (s *Server) getInstance(ctx context.Context, name string) (_map.MapServiceServer, error) {
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
	instance = proxy.(_map.MapServiceServer)

	if primitiveMeta.Cached {
		cached, err := primitiveType.NewCacheDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = cached.(_map.MapServiceServer)
	}

	if primitiveMeta.ReadOnly {
		readOnly, err := primitiveType.NewReadOnlyDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = readOnly.(_map.MapServiceServer)
	}

	s.instances[name] = instance
	return instance, nil
}

func (s *Server) Size(ctx context.Context, request *_map.SizeRequest) (*_map.SizeResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("SizeRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Size(ctx, request)
}

func (s *Server) Put(ctx context.Context, request *_map.PutRequest) (*_map.PutResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("PutRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Put(ctx, request)
}

func (s *Server) Get(ctx context.Context, request *_map.GetRequest) (*_map.GetResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Get(ctx, request)
}

func (s *Server) Remove(ctx context.Context, request *_map.RemoveRequest) (*_map.RemoveResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("RemoveRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Remove(ctx, request)
}

func (s *Server) Clear(ctx context.Context, request *_map.ClearRequest) (*_map.ClearResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("ClearRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Clear(ctx, request)
}

func (s *Server) Events(request *_map.EventsRequest, srv _map.MapService_EventsServer) error {
	instance, err := s.getInstance(srv.Context(), request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return instance.Events(request, srv)
}

func (s *Server) Entries(request *_map.EntriesRequest, srv _map.MapService_EntriesServer) error {
	instance, err := s.getInstance(srv.Context(), request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EntriesRequest %+v failed: %v", request, err)
		return err
	}
	return instance.Entries(request, srv)
}
