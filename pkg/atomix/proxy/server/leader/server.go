package leader

import (
	"context"
	leader "github.com/atomix/api/go/atomix/primitive/leader"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"google.golang.org/grpc"
	"sync"
)

const Type = "LeaderLatch"

func RegisterService(node proxy.Node) {
	node.Services().RegisterService(func(s *grpc.Server) {
		server := &Server{
			node:      node,
			instances: make(map[string]leader.LeaderLatchServiceServer),
			log:       logging.GetLogger("atomix", "leaderlatch"),
		}
		leader.RegisterLeaderLatchServiceServer(s, server)
	})
}

type Server struct {
	node      proxy.Node
	instances map[string]leader.LeaderLatchServiceServer
	mu        sync.RWMutex
	log       logging.Logger
}

func (s *Server) getInstance(name string) (leader.LeaderLatchServiceServer, error) {
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
	instance = proxy.(leader.LeaderLatchServiceServer)

	if primitiveMeta.Cached {
		cached, err := primitiveType.NewCacheDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = cached.(leader.LeaderLatchServiceServer)
	}

	if primitiveMeta.ReadOnly {
		readOnly, err := primitiveType.NewReadOnlyDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = readOnly.(leader.LeaderLatchServiceServer)
	}

	s.instances[name] = instance
	return instance, nil
}

func (s *Server) Latch(ctx context.Context, request *leader.LatchRequest) (*leader.LatchResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("LatchRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Latch(ctx, request)
}

func (s *Server) Get(ctx context.Context, request *leader.GetRequest) (*leader.GetResponse, error) {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("GetRequest %+v failed: %v", request, err)
		return nil, err
	}
	return instance.Get(ctx, request)
}

func (s *Server) Events(request *leader.EventsRequest, srv leader.LeaderLatchService_EventsServer) error {
	instance, err := s.getInstance(request.Headers.PrimitiveID)
	if err != nil {
		s.log.Warnf("EventsRequest %+v failed: %v", request, err)
		return err
	}
	return instance.Events(request, srv)
}
