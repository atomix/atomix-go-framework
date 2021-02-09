package indexedmap

import (
	"context"
	indexedmap "github.com/atomix/api/go/atomix/primitive/indexedmap"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"google.golang.org/grpc"
	"sync"
)

const Type = "IndexedMap"

func RegisterService(node proxy.Node) {
	node.Services().RegisterService(func(s *grpc.Server) {
		server := &Server{
			node:      node,
			instances: make(map[string]indexedmap.IndexedMapServiceServer),
			log:       logging.GetLogger("atomix", "indexedmap"),
		}
		indexedmap.RegisterIndexedMapServiceServer(s, server)
	})
}

type Server struct {
	node      proxy.Node
	instances map[string]indexedmap.IndexedMapServiceServer
	mu        sync.RWMutex
	log       logging.Logger
}

func (s *Server) getInstance(name string) (indexedmap.IndexedMapServiceServer, error) {
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

	primitiveMeta, err := s.node.Primitives().GetPrimitive(name)
	if err != nil {
		return nil, err
	}

	proxy, err := primitiveType.NewProxy()
	if err != nil {
		return nil, err
	}
	instance = proxy.(indexedmap.IndexedMapServiceServer)

	if primitiveMeta.Cached {
		cached, err := primitiveType.NewCacheDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = cached.(indexedmap.IndexedMapServiceServer)
	}

	if primitiveMeta.ReadOnly {
		readOnly, err := primitiveType.NewReadOnlyDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = readOnly.(indexedmap.IndexedMapServiceServer)
	}

	s.instances[name] = instance
	return instance, nil
}

func (s *Server) Size(ctx context.Context, request *indexedmap.SizeRequest) (*indexedmap.SizeResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("SizeRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Size(ctx, request)
}

func (s *Server) Put(ctx context.Context, request *indexedmap.PutRequest) (*indexedmap.PutResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("PutRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Put(ctx, request)
}

func (s *Server) Get(ctx context.Context, request *indexedmap.GetRequest) (*indexedmap.GetResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Get(ctx, request)
}

func (s *Server) FirstEntry(ctx context.Context, request *indexedmap.FirstEntryRequest) (*indexedmap.FirstEntryResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("FirstEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.FirstEntry(ctx, request)
}

func (s *Server) LastEntry(ctx context.Context, request *indexedmap.LastEntryRequest) (*indexedmap.LastEntryResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("LastEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.LastEntry(ctx, request)
}

func (s *Server) PrevEntry(ctx context.Context, request *indexedmap.PrevEntryRequest) (*indexedmap.PrevEntryResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("PrevEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.PrevEntry(ctx, request)
}

func (s *Server) NextEntry(ctx context.Context, request *indexedmap.NextEntryRequest) (*indexedmap.NextEntryResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("NextEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.NextEntry(ctx, request)
}

func (s *Server) Remove(ctx context.Context, request *indexedmap.RemoveRequest) (*indexedmap.RemoveResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("RemoveRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Remove(ctx, request)
}

func (s *Server) Clear(ctx context.Context, request *indexedmap.ClearRequest) (*indexedmap.ClearResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("ClearRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Clear(ctx, request)
}

func (s *Server) Events(request *indexedmap.EventsRequest, srv indexedmap.IndexedMapService_EventsServer) error {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return instance.Events(request, srv)
}

func (s *Server) Entries(request *indexedmap.EntriesRequest, srv indexedmap.IndexedMapService_EntriesServer) error {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EntriesRequest %+v failed: %v", request, err)
		return err
	}
	return instance.Entries(request, srv)
}
