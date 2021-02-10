package log

import (
	"context"
	log "github.com/atomix/api/go/atomix/primitive/log"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"google.golang.org/grpc"
	"sync"
)

const Type = "Log"

func RegisterService(node proxy.Node) {
	node.Services().RegisterService(func(s *grpc.Server) {
		server := &Server{
			node:      node,
			instances: make(map[string]log.LogServiceServer),
			log:       logging.GetLogger("atomix", "log"),
		}
		log.RegisterLogServiceServer(s, server)
	})
}

type Server struct {
	node      proxy.Node
	instances map[string]log.LogServiceServer
	mu        sync.RWMutex
	log       logging.Logger
}

func (s *Server) getInstance(ctx context.Context, name string) (log.LogServiceServer, error) {
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
	instance = proxy.(log.LogServiceServer)

	if primitiveMeta.Cached {
		cached, err := primitiveType.NewCacheDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = cached.(log.LogServiceServer)
	}

	if primitiveMeta.ReadOnly {
		readOnly, err := primitiveType.NewReadOnlyDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = readOnly.(log.LogServiceServer)
	}

	s.instances[name] = instance
	return instance, nil
}

func (s *Server) Size(ctx context.Context, request *log.SizeRequest) (*log.SizeResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("SizeRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Size(ctx, request)
}

func (s *Server) Append(ctx context.Context, request *log.AppendRequest) (*log.AppendResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("AppendRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Append(ctx, request)
}

func (s *Server) Get(ctx context.Context, request *log.GetRequest) (*log.GetResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Get(ctx, request)
}

func (s *Server) FirstEntry(ctx context.Context, request *log.FirstEntryRequest) (*log.FirstEntryResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("FirstEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.FirstEntry(ctx, request)
}

func (s *Server) LastEntry(ctx context.Context, request *log.LastEntryRequest) (*log.LastEntryResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("LastEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.LastEntry(ctx, request)
}

func (s *Server) PrevEntry(ctx context.Context, request *log.PrevEntryRequest) (*log.PrevEntryResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("PrevEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.PrevEntry(ctx, request)
}

func (s *Server) NextEntry(ctx context.Context, request *log.NextEntryRequest) (*log.NextEntryResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("NextEntryRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.NextEntry(ctx, request)
}

func (s *Server) Remove(ctx context.Context, request *log.RemoveRequest) (*log.RemoveResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("RemoveRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Remove(ctx, request)
}

func (s *Server) Clear(ctx context.Context, request *log.ClearRequest) (*log.ClearResponse, error) {
	instance, err := s.getInstance(ctx, request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("ClearRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Clear(ctx, request)
}

func (s *Server) Events(request *log.EventsRequest, srv log.LogService_EventsServer) error {
	instance, err := s.getInstance(srv.Context(), request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return instance.Events(request, srv)
}

func (s *Server) Entries(request *log.EntriesRequest, srv log.LogService_EntriesServer) error {
	instance, err := s.getInstance(srv.Context(), request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EntriesRequest %+v failed: %v", request, err)
		return err
	}
	return instance.Entries(request, srv)
}
