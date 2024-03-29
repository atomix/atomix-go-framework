// Code generated by atomix-go-framework. DO NOT EDIT.

// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"context"
	indexedmap "github.com/atomix/atomix-api/go/atomix/primitive/indexedmap"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	storage "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
)

const Type = "IndexedMap"

const (
	sizeOp       storage.OperationID = 1
	putOp        storage.OperationID = 2
	getOp        storage.OperationID = 3
	firstEntryOp storage.OperationID = 4
	lastEntryOp  storage.OperationID = 5
	prevEntryOp  storage.OperationID = 6
	nextEntryOp  storage.OperationID = 7
	removeOp     storage.OperationID = 8
	clearOp      storage.OperationID = 9
	eventsOp     storage.OperationID = 10
	entriesOp    storage.OperationID = 11
)

var log = logging.GetLogger("atomix", "proxy", "indexedmap")

// NewProxyServer creates a new ProxyServer
func NewProxyServer(client *rsm.Client, readSync bool) indexedmap.IndexedMapServiceServer {
	return &ProxyServer{
		Client:   client,
		readSync: readSync,
	}
}

type ProxyServer struct {
	*rsm.Client
	readSync bool
	log      logging.Logger
}

func (s *ProxyServer) Size(ctx context.Context, request *indexedmap.SizeRequest) (*indexedmap.SizeResponse, error) {
	log.Debugf("Received SizeRequest %.250s", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoQuery(ctx, sizeOp, input, s.readSync)
	if err != nil {
		log.Debugf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.SizeResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending SizeResponse %.250s", response)
	return response, nil
}

func (s *ProxyServer) Put(ctx context.Context, request *indexedmap.PutRequest) (*indexedmap.PutResponse, error) {
	log.Debugf("Received PutRequest %.250s", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request PutRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request PutRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoCommand(ctx, putOp, input)
	if err != nil {
		log.Debugf("Request PutRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.PutResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request PutRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending PutResponse %.250s", response)
	return response, nil
}

func (s *ProxyServer) Get(ctx context.Context, request *indexedmap.GetRequest) (*indexedmap.GetResponse, error) {
	log.Debugf("Received GetRequest %.250s", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoQuery(ctx, getOp, input, s.readSync)
	if err != nil {
		log.Debugf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.GetResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending GetResponse %.250s", response)
	return response, nil
}

func (s *ProxyServer) FirstEntry(ctx context.Context, request *indexedmap.FirstEntryRequest) (*indexedmap.FirstEntryResponse, error) {
	log.Debugf("Received FirstEntryRequest %.250s", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request FirstEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request FirstEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoQuery(ctx, firstEntryOp, input, s.readSync)
	if err != nil {
		log.Debugf("Request FirstEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.FirstEntryResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request FirstEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending FirstEntryResponse %.250s", response)
	return response, nil
}

func (s *ProxyServer) LastEntry(ctx context.Context, request *indexedmap.LastEntryRequest) (*indexedmap.LastEntryResponse, error) {
	log.Debugf("Received LastEntryRequest %.250s", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request LastEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request LastEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoQuery(ctx, lastEntryOp, input, s.readSync)
	if err != nil {
		log.Debugf("Request LastEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.LastEntryResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request LastEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending LastEntryResponse %.250s", response)
	return response, nil
}

func (s *ProxyServer) PrevEntry(ctx context.Context, request *indexedmap.PrevEntryRequest) (*indexedmap.PrevEntryResponse, error) {
	log.Debugf("Received PrevEntryRequest %.250s", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request PrevEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request PrevEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoQuery(ctx, prevEntryOp, input, s.readSync)
	if err != nil {
		log.Debugf("Request PrevEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.PrevEntryResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request PrevEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending PrevEntryResponse %.250s", response)
	return response, nil
}

func (s *ProxyServer) NextEntry(ctx context.Context, request *indexedmap.NextEntryRequest) (*indexedmap.NextEntryResponse, error) {
	log.Debugf("Received NextEntryRequest %.250s", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request NextEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request NextEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoQuery(ctx, nextEntryOp, input, s.readSync)
	if err != nil {
		log.Debugf("Request NextEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.NextEntryResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request NextEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending NextEntryResponse %.250s", response)
	return response, nil
}

func (s *ProxyServer) Remove(ctx context.Context, request *indexedmap.RemoveRequest) (*indexedmap.RemoveResponse, error) {
	log.Debugf("Received RemoveRequest %.250s", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoCommand(ctx, removeOp, input)
	if err != nil {
		log.Debugf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.RemoveResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending RemoveResponse %.250s", response)
	return response, nil
}

func (s *ProxyServer) Clear(ctx context.Context, request *indexedmap.ClearRequest) (*indexedmap.ClearResponse, error) {
	log.Debugf("Received ClearRequest %.250s", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request ClearRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request ClearRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoCommand(ctx, clearOp, input)
	if err != nil {
		log.Debugf("Request ClearRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.ClearResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request ClearRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending ClearResponse %.250s", response)
	return response, nil
}

func (s *ProxyServer) Events(request *indexedmap.EventsRequest, srv indexedmap.IndexedMapService_EventsServer) error {
	log.Debugf("Received EventsRequest %.250s", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(srv.Context(), serviceInfo)
	if err != nil {
		return err
	}

	stream := streams.NewBufferedStream()
	err = service.DoCommandStream(srv.Context(), eventsOp, input, stream)
	if err != nil {
		log.Debugf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	ch := make(chan streams.Result)
	go func() {
		defer close(ch)
		for {
			result, ok := stream.Receive()
			if !ok {
				return
			}
			ch <- result
		}
	}()

	for result := range ch {
		if result.Failed() {
			if result.Error == context.Canceled {
				break
			}
			log.Debugf("Request EventsRequest failed: %v", result.Error)
			return errors.Proto(result.Error)
		}

		response := &indexedmap.EventsResponse{}
		err = proto.Unmarshal(result.Value.([]byte), response)
		if err != nil {
			log.Errorf("Request EventsRequest failed: %v", err)
			return errors.Proto(err)
		}

		log.Debugf("Sending EventsResponse %.250s", response)
		if err = srv.Send(response); err != nil {
			log.Warnf("Response EventsResponse failed: %v", err)
			return err
		}
	}
	log.Debugf("Finished EventsRequest %.250s", request)
	return nil
}

func (s *ProxyServer) Entries(request *indexedmap.EntriesRequest, srv indexedmap.IndexedMapService_EntriesServer) error {
	log.Debugf("Received EntriesRequest %.250s", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request EntriesRequest failed: %v", err)
		return errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(srv.Context(), serviceInfo)
	if err != nil {
		return err
	}

	stream := streams.NewBufferedStream()
	err = service.DoQueryStream(srv.Context(), entriesOp, input, stream, s.readSync)
	if err != nil {
		log.Debugf("Request EntriesRequest failed: %v", err)
		return errors.Proto(err)
	}

	ch := make(chan streams.Result)
	go func() {
		defer close(ch)
		for {
			result, ok := stream.Receive()
			if !ok {
				return
			}
			ch <- result
		}
	}()

	for result := range ch {
		if result.Failed() {
			if result.Error == context.Canceled {
				break
			}
			log.Debugf("Request EntriesRequest failed: %v", result.Error)
			return errors.Proto(result.Error)
		}

		response := &indexedmap.EntriesResponse{}
		err = proto.Unmarshal(result.Value.([]byte), response)
		if err != nil {
			log.Errorf("Request EntriesRequest failed: %v", err)
			return errors.Proto(err)
		}

		log.Debugf("Sending EntriesResponse %.250s", response)
		if err = srv.Send(response); err != nil {
			log.Warnf("Response EntriesResponse failed: %v", err)
			return err
		}
	}
	log.Debugf("Finished EntriesRequest %.250s", request)
	return nil
}
