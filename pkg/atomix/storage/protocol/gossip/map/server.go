package _map

import (
	"context"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip"
	"google.golang.org/grpc"
)

// RegisterServer registers the primitive on the given node
func RegisterServer(node *gossip.Node) {
	node.RegisterServer(func(server *grpc.Server, manager *gossip.Manager) {
		_map.RegisterMapServiceServer(server, newServer(manager))
	})
}

func newServer(manager *gossip.Manager) _map.MapServiceServer {
	return &Server{
		manager: manager,
		log:     logging.GetLogger("atomix", "protocol", "gossip", "map"),
	}
}

type Server struct {
	manager *gossip.Manager
	log     logging.Logger
}

func (s *Server) Size(ctx context.Context, request *_map.SizeRequest) (*_map.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)
	s.manager.AddRequestHeaders(&request.Headers)
	partition, err := s.manager.Partition(gossip.PartitionID(request.Headers.PartitionID))
	if err != nil {
		s.log.Errorf("Request SizeRequest %+v failed: %v", request, err)
		return nil, err
	}

	serviceID := gossip.ServiceId{
		Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(ctx, serviceID)
	if err != nil {
		s.log.Errorf("Request SizeRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response, err := service.(Service).Size(ctx, request)
	if err != nil {
		s.log.Errorf("Request SizeRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	s.manager.AddResponseHeaders(&response.Headers)
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

func (s *Server) Put(ctx context.Context, request *_map.PutRequest) (*_map.PutResponse, error) {
	s.log.Debugf("Received PutRequest %+v", request)
	s.manager.AddRequestHeaders(&request.Headers)
	partition, err := s.manager.Partition(gossip.PartitionID(request.Headers.PartitionID))
	if err != nil {
		s.log.Errorf("Request PutRequest %+v failed: %v", request, err)
		return nil, err
	}

	serviceID := gossip.ServiceId{
		Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(ctx, serviceID)
	if err != nil {
		s.log.Errorf("Request PutRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response, err := service.(Service).Put(ctx, request)
	if err != nil {
		s.log.Errorf("Request PutRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	s.manager.AddResponseHeaders(&response.Headers)
	s.log.Debugf("Sending PutResponse %+v", response)
	return response, nil
}

func (s *Server) Get(ctx context.Context, request *_map.GetRequest) (*_map.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	s.manager.AddRequestHeaders(&request.Headers)
	partition, err := s.manager.Partition(gossip.PartitionID(request.Headers.PartitionID))
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
	s.manager.AddResponseHeaders(&response.Headers)
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Server) Remove(ctx context.Context, request *_map.RemoveRequest) (*_map.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)
	s.manager.AddRequestHeaders(&request.Headers)
	partition, err := s.manager.Partition(gossip.PartitionID(request.Headers.PartitionID))
	if err != nil {
		s.log.Errorf("Request RemoveRequest %+v failed: %v", request, err)
		return nil, err
	}

	serviceID := gossip.ServiceId{
		Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(ctx, serviceID)
	if err != nil {
		s.log.Errorf("Request RemoveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response, err := service.(Service).Remove(ctx, request)
	if err != nil {
		s.log.Errorf("Request RemoveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	s.manager.AddResponseHeaders(&response.Headers)
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *Server) Clear(ctx context.Context, request *_map.ClearRequest) (*_map.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)
	s.manager.AddRequestHeaders(&request.Headers)
	partition, err := s.manager.Partition(gossip.PartitionID(request.Headers.PartitionID))
	if err != nil {
		s.log.Errorf("Request ClearRequest %+v failed: %v", request, err)
		return nil, err
	}

	serviceID := gossip.ServiceId{
		Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(ctx, serviceID)
	if err != nil {
		s.log.Errorf("Request ClearRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response, err := service.(Service).Clear(ctx, request)
	if err != nil {
		s.log.Errorf("Request ClearRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	s.manager.AddResponseHeaders(&response.Headers)
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

func (s *Server) Events(request *_map.EventsRequest, srv _map.MapService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)
	s.manager.AddRequestHeaders(&request.Headers)

	partition, err := s.manager.Partition(gossip.PartitionID(request.Headers.PartitionID))
	if err != nil {
		s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}

	serviceID := gossip.ServiceId{
		Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(srv.Context(), serviceID)
	if err != nil {
		s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
		return err
	}

	responseCh := make(chan _map.EventsResponse)
	errCh := make(chan error)
	go func() {
		err := service.(Service).Events(srv.Context(), request, responseCh)
		if err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	for {
		select {
		case response, ok := <-responseCh:
			if ok {
				s.manager.AddResponseHeaders(&response.Headers)
				s.log.Debugf("Sending EventsResponse %v", response)
				err = srv.Send(&response)
				if err != nil {
					s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
					return errors.Proto(err)
				}
			} else {
				s.log.Debugf("Finished EventsRequest %+v", request)
				return nil
			}
		case <-srv.Context().Done():
			s.log.Debugf("Finished EventsRequest %+v", request)
			return nil
		}
	}
}

func (s *Server) Entries(request *_map.EntriesRequest, srv _map.MapService_EntriesServer) error {
	s.log.Debugf("Received EntriesRequest %+v", request)
	s.manager.AddRequestHeaders(&request.Headers)

	partition, err := s.manager.Partition(gossip.PartitionID(request.Headers.PartitionID))
	if err != nil {
		s.log.Errorf("Request EntriesRequest %+v failed: %v", request, err)
		return errors.Proto(err)
	}

	serviceID := gossip.ServiceId{
		Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(srv.Context(), serviceID)
	if err != nil {
		s.log.Errorf("Request EntriesRequest %+v failed: %v", request, err)
		return err
	}

	responseCh := make(chan _map.EntriesResponse)
	errCh := make(chan error)
	go func() {
		err := service.(Service).Entries(srv.Context(), request, responseCh)
		if err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	for {
		select {
		case response, ok := <-responseCh:
			if ok {
				s.manager.AddResponseHeaders(&response.Headers)
				s.log.Debugf("Sending EntriesResponse %v", response)
				err = srv.Send(&response)
				if err != nil {
					s.log.Errorf("Request EntriesRequest %+v failed: %v", request, err)
					return errors.Proto(err)
				}
			} else {
				s.log.Debugf("Finished EntriesRequest %+v", request)
				return nil
			}
		case <-srv.Context().Done():
			s.log.Debugf("Finished EntriesRequest %+v", request)
			return nil
		}
	}
}
