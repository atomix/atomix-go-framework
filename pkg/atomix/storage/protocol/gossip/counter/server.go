// Code generated by atomix-go-framework. DO NOT EDIT.

// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"context"
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	counter "github.com/atomix/atomix-api/go/atomix/primitive/counter"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip"
	"github.com/atomix/atomix-go-framework/pkg/atomix/time"
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

func (s *Server) addRequestHeaders(service Service, headers *primitiveapi.RequestHeaders) {
	var timestamp time.Timestamp
	if headers.Timestamp != nil {
		timestamp = service.Protocol().Clock().Update(time.NewTimestamp(*headers.Timestamp))
	} else {
		timestamp = service.Protocol().Clock().Increment()
	}
	timestampProto := service.Protocol().Clock().Scheme().Codec().EncodeTimestamp(timestamp)
	headers.Timestamp = &timestampProto
}

func (s *Server) addResponseHeaders(service Service, headers *primitiveapi.ResponseHeaders) {
	var timestamp time.Timestamp
	if headers.Timestamp != nil {
		timestamp = service.Protocol().Clock().Update(time.NewTimestamp(*headers.Timestamp))
	} else {
		timestamp = service.Protocol().Clock().Increment()
	}
	timestampProto := service.Protocol().Clock().Scheme().Codec().EncodeTimestamp(timestamp)
	headers.Timestamp = &timestampProto
}

func (s *Server) Set(ctx context.Context, request *counter.SetRequest) (*counter.SetResponse, error) {
	s.log.Debugf("Received SetRequest %+v", request)
	partition, err := s.manager.PartitionFrom(ctx)
	if err != nil {
		s.log.Errorf("Request SetRequest %+v failed: %v", request, err)
		return nil, err
	}

	serviceID := gossip.ServiceId{
		Type:    gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Cluster: request.Headers.ClusterKey,
		Name:    request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(ctx, serviceID, request.Headers.Timestamp)
	if err != nil {
		s.log.Errorf("Request SetRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	s.addRequestHeaders(service.(Service), &request.Headers)
	response, err := service.(Service).Set(ctx, request)
	if err != nil {
		s.log.Errorf("Request SetRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	s.addResponseHeaders(service.(Service), &response.Headers)
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

	serviceID := gossip.ServiceId{
		Type:    gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Cluster: request.Headers.ClusterKey,
		Name:    request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(ctx, serviceID, request.Headers.Timestamp)
	if err != nil {
		s.log.Errorf("Request GetRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	s.addRequestHeaders(service.(Service), &request.Headers)
	response, err := service.(Service).Get(ctx, request)
	if err != nil {
		s.log.Errorf("Request GetRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	s.addResponseHeaders(service.(Service), &response.Headers)
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

	serviceID := gossip.ServiceId{
		Type:    gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Cluster: request.Headers.ClusterKey,
		Name:    request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(ctx, serviceID, request.Headers.Timestamp)
	if err != nil {
		s.log.Errorf("Request IncrementRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	s.addRequestHeaders(service.(Service), &request.Headers)
	response, err := service.(Service).Increment(ctx, request)
	if err != nil {
		s.log.Errorf("Request IncrementRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	s.addResponseHeaders(service.(Service), &response.Headers)
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

	serviceID := gossip.ServiceId{
		Type:    gossip.ServiceType(request.Headers.PrimitiveID.Type),
		Cluster: request.Headers.ClusterKey,
		Name:    request.Headers.PrimitiveID.Name,
	}

	service, err := partition.GetService(ctx, serviceID, request.Headers.Timestamp)
	if err != nil {
		s.log.Errorf("Request DecrementRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	s.addRequestHeaders(service.(Service), &request.Headers)
	response, err := service.(Service).Decrement(ctx, request)
	if err != nil {
		s.log.Errorf("Request DecrementRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	s.addResponseHeaders(service.(Service), &response.Headers)
	s.log.Debugf("Sending DecrementResponse %+v", response)
	return response, nil
}
