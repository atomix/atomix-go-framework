package counter

import (
	"context"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip"
	"google.golang.org/grpc"
)

// RegisterServer registers the primitive on the given node
func RegisterServer(node *gossip.Node) {
	node.RegisterServer(func(server *grpc.Server, manager *gossip.Manager) {
		counter.RegisterCounterServiceServer(server, newServer(manager))
	})
}

func newServer(manager *gossip.Manager) counter.CounterServiceServer {
	return &Server{
		manager: manager,
		log:     logging.GetLogger("atomix", "protocol", "gossip", "counter"),
	}
}

type Server struct {
	manager *gossip.Manager
	log     logging.Logger
}

func (s *Server) Set(ctx context.Context, request *counter.SetRequest) (*counter.SetResponse, error) {
	s.log.Debugf("Received SetRequest %+v", request)
	s.manager.PrepareRequest(&request.Headers)
	partition, err := s.manager.PartitionFrom(ctx)
	if err != nil {
		s.log.Errorf("Request SetRequest %+v failed: %v", request, err)
		return nil, err
	}

	serviceID := gossip.ServiceId{
		Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(ctx, serviceID)
	if err != nil {
		s.log.Errorf("Request SetRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response, err := service.(Service).Set(ctx, request)
	if err != nil {
		s.log.Errorf("Request SetRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	s.manager.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

func (s *Server) Get(ctx context.Context, request *counter.GetRequest) (*counter.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	s.manager.PrepareRequest(&request.Headers)
	partition, err := s.manager.PartitionFrom(ctx)
	if err != nil {
		s.log.Errorf("Request GetRequest %+v failed: %v", request, err)
		return nil, err
	}

	serviceID := gossip.ServiceId{
		Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(ctx, serviceID)
	if err != nil {
		s.log.Errorf("Request GetRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response, err := service.(Service).Get(ctx, request)
	if err != nil {
		s.log.Errorf("Request GetRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	s.manager.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Server) Increment(ctx context.Context, request *counter.IncrementRequest) (*counter.IncrementResponse, error) {
	s.log.Debugf("Received IncrementRequest %+v", request)
	s.manager.PrepareRequest(&request.Headers)
	partition, err := s.manager.PartitionFrom(ctx)
	if err != nil {
		s.log.Errorf("Request IncrementRequest %+v failed: %v", request, err)
		return nil, err
	}

	serviceID := gossip.ServiceId{
		Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(ctx, serviceID)
	if err != nil {
		s.log.Errorf("Request IncrementRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response, err := service.(Service).Increment(ctx, request)
	if err != nil {
		s.log.Errorf("Request IncrementRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	s.manager.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending IncrementResponse %+v", response)
	return response, nil
}

func (s *Server) Decrement(ctx context.Context, request *counter.DecrementRequest) (*counter.DecrementResponse, error) {
	s.log.Debugf("Received DecrementRequest %+v", request)
	s.manager.PrepareRequest(&request.Headers)
	partition, err := s.manager.PartitionFrom(ctx)
	if err != nil {
		s.log.Errorf("Request DecrementRequest %+v failed: %v", request, err)
		return nil, err
	}

	serviceID := gossip.ServiceId{
		Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(ctx, serviceID)
	if err != nil {
		s.log.Errorf("Request DecrementRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response, err := service.(Service).Decrement(ctx, request)
	if err != nil {
		s.log.Errorf("Request DecrementRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	s.manager.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending DecrementResponse %+v", response)
	return response, nil
}
