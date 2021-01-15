package _map

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc"
	"io"
)

// RegisterServer registers the primitive on the given node
func RegisterServer(node *gossip.Node) {
	node.RegisterServer(func(server *grpc.Server, manager *gossip.Manager) {
		_map.RegisterMapServiceServer(server, newServer(newManager(manager)))
	})
	node.RegisterServer(registerServerFunc)
}

var registerServerFunc gossip.RegisterServerFunc

func newServer(manager *Manager) _map.MapServiceServer {
	return &Server{
		manager: manager,
		log:     logging.GetLogger("atomix", "protocol", "gossip", "map"),
	}
}

type Server struct {
	manager *Manager
	log     logging.Logger
}

func (s *Server) Size(ctx context.Context, request *_map.SizeRequest) (*_map.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)
	partitions, err := s.manager.PartitionsFrom(ctx)
	if err != nil {
		s.log.Errorf("Request SizeRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	outputs, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		partition := partitions[i]
		service, err := partition.GetService(request.Header.PrimitiveID.Name)
		if err != nil {
			return nil, err
		}
		return service.Size(ctx)
	})
	if err != nil {
		s.log.Errorf("Request SizeRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &_map.SizeResponse{}
	for _, o := range outputs {
		response.Output.Size_ += o.(*_map.SizeOutput).Size_
	}
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

func (s *Server) Put(ctx context.Context, request *_map.PutRequest) (*_map.PutResponse, error) {
	s.log.Debugf("Received PutRequest %+v", request)
	partition, err := s.manager.PartitionFrom(ctx)
	if err != nil {
		s.log.Errorf("Request PutRequest %+v failed: %v", request, err)
		return nil, err
	}

	service, err := partition.GetService(request.Header.PrimitiveID.Name)
	if err != nil {
		s.log.Errorf("Request PutRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	input := &request.Input
	output, err := service.Put(ctx, input)
	if err != nil {
		s.log.Errorf("Request PutRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &_map.PutResponse{}
	response.Output = *output
	s.log.Debugf("Sending PutResponse %+v", response)
	return response, nil
}

func (s *Server) Get(ctx context.Context, request *_map.GetRequest) (*_map.GetResponse, error) {
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

	response := &_map.GetResponse{}
	response.Output = *output
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Server) Remove(ctx context.Context, request *_map.RemoveRequest) (*_map.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)
	partition, err := s.manager.PartitionFrom(ctx)
	if err != nil {
		s.log.Errorf("Request RemoveRequest %+v failed: %v", request, err)
		return nil, err
	}

	service, err := partition.GetService(request.Header.PrimitiveID.Name)
	if err != nil {
		s.log.Errorf("Request RemoveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	input := &request.Input
	output, err := service.Remove(ctx, input)
	if err != nil {
		s.log.Errorf("Request RemoveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &_map.RemoveResponse{}
	response.Output = *output
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *Server) Clear(ctx context.Context, request *_map.ClearRequest) (*_map.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)
	partitions, err := s.manager.PartitionsFrom(ctx)
	if err != nil {
		s.log.Errorf("Request ClearRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	err = async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		service, err := partition.GetService(request.Header.PrimitiveID.Name)
		if err != nil {
			return err
		}
		return service.Clear(ctx)
	})
	if err != nil {
		s.log.Errorf("Request ClearRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &_map.ClearResponse{}
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

func (s *Server) Events(request *_map.EventsRequest, srv _map.MapService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)

	stream := streams.NewBufferedStream()
	partitions, err := s.manager.PartitionsFrom(srv.Context())
	if err != nil {
		s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}
	input := &request.Input
	err = async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		service, err := partition.GetService(request.Header.PrimitiveID.Name)
		if err != nil {
			return err
		}
		return service.Events(srv.Context(), input, newServiceEventsStream(stream))
	})
	if err != nil {
		s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}

	response := &_map.EventsResponse{}
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

		response := &_map.EventsResponse{}
		response.Output = *result.Value.(*_map.EventsOutput)

		err = srv.Send(response)
		if err != nil {
			s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
			return errors.Proto(err)
		}
	}

	s.log.Debugf("Finished EventsRequest %+v", request)
	return nil
}

func (s *Server) Entries(request *_map.EntriesRequest, srv _map.MapService_EntriesServer) error {
	s.log.Debugf("Received EntriesRequest %+v", request)

	stream := streams.NewBufferedStream()
	partitions, err := s.manager.PartitionsFrom(srv.Context())
	if err != nil {
		s.log.Errorf("Request EntriesRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}
	input := &request.Input
	err = async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		service, err := partition.GetService(request.Header.PrimitiveID.Name)
		if err != nil {
			return err
		}
		return service.Entries(srv.Context(), input, newServiceEntriesStream(stream))
	})
	if err != nil {
		s.log.Errorf("Request EntriesRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}

	response := &_map.EntriesResponse{}
	response.Header = primitiveapi.ResponseHeader{
		ResponseType: primitiveapi.ResponseType_RESPONSE_STREAM,
	}
	err = srv.Send(response)
	if err != nil {
		s.log.Errorf("Request EntriesRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request EntriesRequest %+v failed: %v", request, result.Error)
			return errors.Proto(result.Error)
		}

		response := &_map.EntriesResponse{}
		response.Output = *result.Value.(*_map.EntriesOutput)

		err = srv.Send(response)
		if err != nil {
			s.log.Errorf("Request EntriesRequest %+v failed: %v", request, err)
			return errors.Proto(err)
		}
	}

	s.log.Debugf("Finished EntriesRequest %+v", request)
	return nil
}

func (s *Server) Snapshot(request *_map.SnapshotRequest, srv _map.MapService_SnapshotServer) error {
	s.log.Debugf("Received SnapshotRequest %+v", request)

	stream := streams.NewBufferedStream()
	partitions, err := s.manager.PartitionsFrom(srv.Context())
	if err != nil {
		s.log.Errorf("Request SnapshotRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}
	err = async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		service, err := partition.GetService(request.Header.PrimitiveID.Name)
		if err != nil {
			return err
		}
		return service.Snapshot(srv.Context(), newServiceSnapshotStreamWriter(stream))
	})
	if err != nil {
		s.log.Errorf("Request SnapshotRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}

	response := &_map.SnapshotResponse{}
	response.Header = primitiveapi.ResponseHeader{
		ResponseType: primitiveapi.ResponseType_RESPONSE_STREAM,
	}
	err = srv.Send(response)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request SnapshotRequest %+v failed: %v", request, result.Error)
			return errors.Proto(result.Error)
		}

		response := &_map.SnapshotResponse{}
		response.Entry = *result.Value.(*_map.SnapshotEntry)

		err = srv.Send(response)
		if err != nil {
			s.log.Errorf("Request SnapshotRequest %+v failed: %v", request, err)
			return errors.Proto(err)
		}
	}

	s.log.Debugf("Finished SnapshotRequest %+v", request)
	return nil
}

func (s *Server) Restore(srv _map.MapService_RestoreServer) error {
	response := &_map.RestoreResponse{}
	for {
		request, err := srv.Recv()
		if err == io.EOF {
			s.log.Debugf("Sending RestoreResponse %+v", response)
			return srv.SendAndClose(response)
		} else if err != nil {
			s.log.Errorf("Request RestoreRequest %+v failed: %v", request, err)
			return errors.Proto(err)
		}

		s.log.Debugf("Received RestoreRequest %+v", request)
		partitions, err := s.manager.PartitionsFrom(srv.Context())
		if err != nil {
			s.log.Errorf("Request RestoreRequest %+v failed: %v", request, err)
			return errors.Proto(err)
		}
		input := &request.Entry
		err = async.IterAsync(len(partitions), func(i int) error {
			partition := partitions[i]
			service, err := partition.GetService(request.Header.PrimitiveID.Name)
			if err != nil {
				return err
			}
			return service.Restore(srv.Context(), input)
		})
		if err != nil {
			s.log.Errorf("Request RestoreRequest %+v failed: %v", request, err)
			return errors.Proto(err)
		}

		response = &_map.RestoreResponse{}
	}
}
