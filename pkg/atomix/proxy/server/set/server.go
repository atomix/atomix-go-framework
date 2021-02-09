package set

import (
	"context"
	set "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"google.golang.org/grpc"
	"sync"
)

const Type = "Set"

func RegisterService(node proxy.Node) {
	node.Services().RegisterService(func(s *grpc.Server) {
		server := &Server{
			node:      node,
			instances: make(map[string]set.SetServiceServer),
			log:       logging.GetLogger("atomix", "set"),
		}
		set.RegisterSetServiceServer(s, server)
	})
}

type Server struct {
	node      proxy.Node
	instances map[string]set.SetServiceServer
	mu        sync.RWMutex
	log       logging.Logger
}

func (s *Server) getInstance(name string) (set.SetServiceServer, error) {
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
	instance = proxy.(set.SetServiceServer)

	if primitiveMeta.Cached {
		cached, err := primitiveType.NewCacheDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = cached.(set.SetServiceServer)
	}

	if primitiveMeta.ReadOnly {
		readOnly, err := primitiveType.NewReadOnlyDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = readOnly.(set.SetServiceServer)
	}

	s.instances[name] = instance
	return instance, nil
}

func (s *Server) Size(ctx context.Context, request *set.SizeRequest) (*set.SizeResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("SizeRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Size(ctx, request)
}

func (s *Server) Contains(ctx context.Context, request *set.ContainsRequest) (*set.ContainsResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("ContainsRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Contains(ctx, request)
}

func (s *Server) Add(ctx context.Context, request *set.AddRequest) (*set.AddResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("AddRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Add(ctx, request)
}

func (s *Server) Remove(ctx context.Context, request *set.RemoveRequest) (*set.RemoveResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("RemoveRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Remove(ctx, request)
}

func (s *Server) Clear(ctx context.Context, request *set.ClearRequest) (*set.ClearResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("ClearRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Clear(ctx, request)
}

func (s *Server) Events(request *set.EventsRequest, srv set.SetService_EventsServer) error {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return instance.Events(request, srv)
}

func (s *Server) Elements(request *set.ElementsRequest, srv set.SetService_ElementsServer) error {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("ElementsRequest %+v failed: %v", request, err)
		return err
	}
	return instance.Elements(request, srv)
}
