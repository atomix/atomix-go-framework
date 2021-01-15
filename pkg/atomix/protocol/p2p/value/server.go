package value

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/protocol/p2p"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"google.golang.org/grpc"
)

// RegisterServer registers the primitive on the given node
func RegisterServer(node *p2p.Node) {
	node.RegisterServer(func(server *grpc.Server, manager *p2p.Manager) {
		value.RegisterValueServiceServer(server, newServer(newManager(manager)))
	})
	node.RegisterServer(registerServerFunc)
}

var registerServerFunc p2p.RegisterServerFunc

func newServer(manager *Manager) value.ValueServiceServer {
	return &Server{
		manager: manager,
		log:     logging.GetLogger("atomix", "protocol", "p2p", "value"),
	}
}

type Server struct {
	manager *Manager
	log     logging.Logger
}

func (s *Server) Set(ctx context.Context, request *value.SetRequest) (*value.SetResponse, error) {
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

	response := &value.SetResponse{}
	response.Output = *output
	s.log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

func (s *Server) Get(ctx context.Context, request *value.GetRequest) (*value.GetResponse, error) {
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

	response := &value.GetResponse{}
	response.Output = *output
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Server) Events(request *value.EventsRequest, srv value.ValueService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)

	stream := streams.NewBufferedStream()
	partition, err := s.manager.PartitionFrom(srv.Context())
	if err != nil {
		s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}

	service, err := partition.GetService(request.Header.PrimitiveID.Name)
	if err != nil {
		s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}
	input := &request.Input
	err = service.Events(srv.Context(), input, newServiceEventsStream(stream))
	if err != nil {
		s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}

	response := &value.EventsResponse{}
	response.Header = primitiveapi.ResponseHeader{
		ResponseType: primitiveapi.ResponseType_RESPONSE_STREAM,
	}
	err = srv.Send(response)
	if err != nil {
		s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request EventsRequest %+v failed: %v", request, result.Error)
			return errors.Proto(result.Error)
		}

		response := &value.EventsResponse{}
		response.Output = *result.Value.(*value.EventsOutput)

		err = srv.Send(response)
		if err != nil {
			s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
			return errors.Proto(err)
		}
	}

	s.log.Debugf("Finished EventsRequest %+v", request)
	return nil
}

func (s *Server) Snapshot(ctx context.Context, request *value.SnapshotRequest) (*value.SnapshotResponse, error) {
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

	response := &value.SnapshotResponse{}
	response.Snapshot = *output
	s.log.Debugf("Sending SnapshotResponse %+v", response)
	return response, nil
}

func (s *Server) Restore(ctx context.Context, request *value.RestoreRequest) (*value.RestoreResponse, error) {
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

	response := &value.RestoreResponse{}
	s.log.Debugf("Sending RestoreResponse %+v", response)
	return response, nil
}
