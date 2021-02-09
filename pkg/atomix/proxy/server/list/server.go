package list

import (
	"context"
	list "github.com/atomix/api/go/atomix/primitive/list"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"google.golang.org/grpc"
	"sync"
)

const Type = "List"

func RegisterService(node proxy.Node) {
	node.Services().RegisterService(func(s *grpc.Server) {
		server := &Server{
			node:      node,
			instances: make(map[string]list.ListServiceServer),
			log:       logging.GetLogger("atomix", "list"),
		}
		list.RegisterListServiceServer(s, server)
	})
}

type Server struct {
	node      proxy.Node
	instances map[string]list.ListServiceServer
	mu        sync.RWMutex
	log       logging.Logger
}

func (s *Server) getInstance(name string) (list.ListServiceServer, error) {
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
	instance = proxy.(list.ListServiceServer)

	if primitiveMeta.Cached {
		cached, err := primitiveType.NewCacheDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = cached.(list.ListServiceServer)
	}

	if primitiveMeta.ReadOnly {
		readOnly, err := primitiveType.NewReadOnlyDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = readOnly.(list.ListServiceServer)
	}

	s.instances[name] = instance
	return instance, nil
}

func (s *Server) Size(ctx context.Context, request *list.SizeRequest) (*list.SizeResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("SizeRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Size(ctx, request)
}

func (s *Server) Append(ctx context.Context, request *list.AppendRequest) (*list.AppendResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("AppendRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Append(ctx, request)
}

func (s *Server) Insert(ctx context.Context, request *list.InsertRequest) (*list.InsertResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("InsertRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Insert(ctx, request)
}

func (s *Server) Get(ctx context.Context, request *list.GetRequest) (*list.GetResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Get(ctx, request)
}

func (s *Server) Set(ctx context.Context, request *list.SetRequest) (*list.SetResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("SetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Set(ctx, request)
}

func (s *Server) Remove(ctx context.Context, request *list.RemoveRequest) (*list.RemoveResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("RemoveRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Remove(ctx, request)
}

func (s *Server) Clear(ctx context.Context, request *list.ClearRequest) (*list.ClearResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("ClearRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Clear(ctx, request)
}

func (s *Server) Events(request *list.EventsRequest, srv list.ListService_EventsServer) error {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return instance.Events(request, srv)
}

func (s *Server) Elements(request *list.ElementsRequest, srv list.ListService_ElementsServer) error {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("ElementsRequest %+v failed: %v", request, err)
		return err
	}
	return instance.Elements(request, srv)
}
