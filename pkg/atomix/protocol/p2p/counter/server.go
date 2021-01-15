package counter

import (
	"context"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/protocol/p2p"
	"google.golang.org/grpc"
)

// RegisterServer registers the primitive on the given node
func RegisterServer(node *p2p.Node) {
	node.RegisterServer(func(server *grpc.Server, manager *p2p.Manager) {
		counter.RegisterCounterServiceServer(server, newServer(newManager(manager)))
	})
	node.RegisterServer(registerServerFunc)
}

var registerServerFunc p2p.RegisterServerFunc

func newServer(manager *Manager) counter.CounterServiceServer {
	return &Server{
		manager: manager,
		log:     logging.GetLogger("atomix", "protocol", "p2p", "counter"),
	}
}

type Server struct {
	manager *Manager
	log     logging.Logger
}

func (s *Server) Set(ctx context.Context, request *counter.SetRequest) (*counter.SetResponse, error) {
	s.log.Debugf("Received SetRequest %+v", request)
	partition, err := s.manager.PartitionFrom(ctx)
	if err != nil {
		s.log.Errorf("Request SetRequest %+v failed: %v", request, err)
		return nil, err
	}

	service, err := partition.GetService(request.Header.PrimitiveID.Name)
	if err != nil {
		s.log.Errorf("Request SetRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	input := &request.Input
	output, err := service.Set(ctx, input)
	if err != nil {
		s.log.Errorf("Request SetRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &counter.SetResponse{}
	response.Output = *output
	s.log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

func (s *Server) Get(ctx context.Context, request *counter.GetRequest) (*counter.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	partition, err := s.manager.PartitionFrom(ctx)
	if err != nil {
		s.log.Errorf("Request GetRequest %+v failed: %v", request, err)
		return nil, err
	}

	service, err := partition.GetService(request.Header.PrimitiveID.Name)
	if err != nil {
		s.log.Errorf("Request GetRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	input := &request.Input
	output, err := service.Get(ctx, input)
	if err != nil {
		s.log.Errorf("Request GetRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &counter.GetResponse{}
	response.Output = *output
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Server) Increment(ctx context.Context, request *counter.IncrementRequest) (*counter.IncrementResponse, error) {
	s.log.Debugf("Received IncrementRequest %+v", request)
	partition, err := s.manager.PartitionFrom(ctx)
	if err != nil {
		s.log.Errorf("Request IncrementRequest %+v failed: %v", request, err)
		return nil, err
	}

	service, err := partition.GetService(request.Header.PrimitiveID.Name)
	if err != nil {
		s.log.Errorf("Request IncrementRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	input := &request.Input
	output, err := service.Increment(ctx, input)
	if err != nil {
		s.log.Errorf("Request IncrementRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &counter.IncrementResponse{}
	response.Output = *output
	s.log.Debugf("Sending IncrementResponse %+v", response)
	return response, nil
}

func (s *Server) Decrement(ctx context.Context, request *counter.DecrementRequest) (*counter.DecrementResponse, error) {
	s.log.Debugf("Received DecrementRequest %+v", request)
	partition, err := s.manager.PartitionFrom(ctx)
	if err != nil {
		s.log.Errorf("Request DecrementRequest %+v failed: %v", request, err)
		return nil, err
	}

	service, err := partition.GetService(request.Header.PrimitiveID.Name)
	if err != nil {
		s.log.Errorf("Request DecrementRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	input := &request.Input
	output, err := service.Decrement(ctx, input)
	if err != nil {
		s.log.Errorf("Request DecrementRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &counter.DecrementResponse{}
	response.Output = *output
	s.log.Debugf("Sending DecrementResponse %+v", response)
	return response, nil
}

func (s *Server) Snapshot(ctx context.Context, request *counter.SnapshotRequest) (*counter.SnapshotResponse, error) {
	s.log.Debugf("Received SnapshotRequest %+v", request)
	partition, err := s.manager.PartitionFrom(ctx)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest %+v failed: %v", request, err)
		return nil, err
	}

	service, err := partition.GetService(request.Header.PrimitiveID.Name)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	output, err := service.Snapshot(ctx)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &counter.SnapshotResponse{}
	response.Snapshot = *output
	s.log.Debugf("Sending SnapshotResponse %+v", response)
	return response, nil
}

func (s *Server) Restore(ctx context.Context, request *counter.RestoreRequest) (*counter.RestoreResponse, error) {
	s.log.Debugf("Received RestoreRequest %+v", request)
	partition, err := s.manager.PartitionFrom(ctx)
	if err != nil {
		s.log.Errorf("Request RestoreRequest %+v failed: %v", request, err)
		return nil, err
	}

	service, err := partition.GetService(request.Header.PrimitiveID.Name)
	if err != nil {
		s.log.Errorf("Request RestoreRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	input := &request.Snapshot
	err = service.Restore(ctx, input)
	if err != nil {
		s.log.Errorf("Request RestoreRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &counter.RestoreResponse{}
	s.log.Debugf("Sending RestoreResponse %+v", response)
	return response, nil
}
