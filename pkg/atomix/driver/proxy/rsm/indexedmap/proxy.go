package indexedmap

import (
	"context"
	indexedmap "github.com/atomix/api/go/atomix/primitive/indexedmap"
	"github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	storage "github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
)

const Type = "IndexedMap"

const (
	sizeOp       = "Size"
	putOp        = "Put"
	getOp        = "Get"
	firstEntryOp = "FirstEntry"
	lastEntryOp  = "LastEntry"
	prevEntryOp  = "PrevEntry"
	nextEntryOp  = "NextEntry"
	removeOp     = "Remove"
	clearOp      = "Clear"
	eventsOp     = "Events"
	entriesOp    = "Entries"
)

// NewProxyServer creates a new ProxyServer
func NewProxyServer(client *rsm.Client) indexedmap.IndexedMapServiceServer {
	return &ProxyServer{
		Client: client,
		log:    logging.GetLogger("atomix", "counter"),
	}
}

type ProxyServer struct {
	*rsm.Client
	log logging.Logger
}

func (s *ProxyServer) Size(ctx context.Context, request *indexedmap.SizeRequest) (*indexedmap.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoQuery(ctx, service, sizeOp, input)
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.SizeResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) Put(ctx context.Context, request *indexedmap.PutRequest) (*indexedmap.PutResponse, error) {
	s.log.Debugf("Received PutRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request PutRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoCommand(ctx, service, putOp, input)
	if err != nil {
		s.log.Errorf("Request PutRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.PutResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request PutRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending PutResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) Get(ctx context.Context, request *indexedmap.GetRequest) (*indexedmap.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoQuery(ctx, service, getOp, input)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.GetResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) FirstEntry(ctx context.Context, request *indexedmap.FirstEntryRequest) (*indexedmap.FirstEntryResponse, error) {
	s.log.Debugf("Received FirstEntryRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request FirstEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoQuery(ctx, service, firstEntryOp, input)
	if err != nil {
		s.log.Errorf("Request FirstEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.FirstEntryResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request FirstEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending FirstEntryResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) LastEntry(ctx context.Context, request *indexedmap.LastEntryRequest) (*indexedmap.LastEntryResponse, error) {
	s.log.Debugf("Received LastEntryRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request LastEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoQuery(ctx, service, lastEntryOp, input)
	if err != nil {
		s.log.Errorf("Request LastEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.LastEntryResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request LastEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending LastEntryResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) PrevEntry(ctx context.Context, request *indexedmap.PrevEntryRequest) (*indexedmap.PrevEntryResponse, error) {
	s.log.Debugf("Received PrevEntryRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request PrevEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoQuery(ctx, service, prevEntryOp, input)
	if err != nil {
		s.log.Errorf("Request PrevEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.PrevEntryResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request PrevEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending PrevEntryResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) NextEntry(ctx context.Context, request *indexedmap.NextEntryRequest) (*indexedmap.NextEntryResponse, error) {
	s.log.Debugf("Received NextEntryRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request NextEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoQuery(ctx, service, nextEntryOp, input)
	if err != nil {
		s.log.Errorf("Request NextEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.NextEntryResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request NextEntryRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending NextEntryResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) Remove(ctx context.Context, request *indexedmap.RemoveRequest) (*indexedmap.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoCommand(ctx, service, removeOp, input)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.RemoveResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) Clear(ctx context.Context, request *indexedmap.ClearRequest) (*indexedmap.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoCommand(ctx, service, clearOp, input)
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &indexedmap.ClearResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) Events(request *indexedmap.EventsRequest, srv indexedmap.IndexedMapService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	err = partition.DoCommandStream(srv.Context(), service, eventsOp, input, stream)
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request EventsRequest failed: %v", result.Error)
			return errors.Proto(result.Error)
		}

		response := &indexedmap.EventsResponse{}
		err = proto.Unmarshal(result.Value.([]byte), response)
		if err != nil {
			s.log.Errorf("Request EventsRequest failed: %v", err)
			return errors.Proto(err)
		}

		s.log.Debugf("Sending EventsResponse %+v", response)
		if err = srv.Send(response); err != nil {
			s.log.Errorf("Response EventsResponse failed: %v", err)
			return errors.Proto(err)
		}
	}
	s.log.Debugf("Finished EventsRequest %+v", request)
	return nil
}

func (s *ProxyServer) Entries(request *indexedmap.EntriesRequest, srv indexedmap.IndexedMapService_EntriesServer) error {
	s.log.Debugf("Received EntriesRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request EntriesRequest failed: %v", err)
		return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	err = partition.DoQueryStream(srv.Context(), service, entriesOp, input, stream)
	if err != nil {
		s.log.Errorf("Request EntriesRequest failed: %v", err)
		return errors.Proto(err)
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request EntriesRequest failed: %v", result.Error)
			return errors.Proto(result.Error)
		}

		response := &indexedmap.EntriesResponse{}
		err = proto.Unmarshal(result.Value.([]byte), response)
		if err != nil {
			s.log.Errorf("Request EntriesRequest failed: %v", err)
			return errors.Proto(err)
		}

		s.log.Debugf("Sending EntriesResponse %+v", response)
		if err = srv.Send(response); err != nil {
			s.log.Errorf("Response EntriesResponse failed: %v", err)
			return errors.Proto(err)
		}
	}
	s.log.Debugf("Finished EntriesRequest %+v", request)
	return nil
}
