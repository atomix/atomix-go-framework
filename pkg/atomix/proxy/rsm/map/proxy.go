package _map

import (
	"context"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	protocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	async "github.com/atomix/go-framework/pkg/atomix/util/async"
	"github.com/golang/protobuf/proto"

	streams "github.com/atomix/go-framework/pkg/atomix/stream"
)

const Type = "Map"

const (
	sizeOp    = "Size"
	putOp     = "Put"
	getOp     = "Get"
	removeOp  = "Remove"
	clearOp   = "Clear"
	eventsOp  = "Events"
	entriesOp = "Entries"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *rsm.Node) {
	node.PrimitiveTypes().RegisterProxyFunc(Type, func() (interface{}, error) {
		return &Proxy{
			Proxy: rsm.NewProxy(node.Client),
			log:   logging.GetLogger("atomix", "map"),
		}, nil
	})
}

type Proxy struct {
	*rsm.Proxy
	log logging.Logger
}

func (s *Proxy) Size(ctx context.Context, request *_map.SizeRequest) (*_map.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partitions := s.Partitions()

	service := protocol.ServiceId{
		Type: Type,
		Name: request.Headers.PrimitiveID,
	}
	outputs, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		return partitions[i].DoQuery(ctx, service, sizeOp, input)
	})
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	responses := make([]_map.SizeResponse, 0, len(outputs))
	for _, output := range outputs {
		var response _map.SizeResponse
		err := proto.Unmarshal(output.([]byte), &response)
		if err != nil {
			s.log.Errorf("Request SizeRequest failed: %v", err)
			return nil, errors.Proto(err)
		}
		responses = append(responses, response)
	}

	response := &responses[0]
	for _, r := range responses {
		response.Size_ += r.Size_
	}
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

func (s *Proxy) Put(ctx context.Context, request *_map.PutRequest) (*_map.PutResponse, error) {
	s.log.Debugf("Received PutRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request PutRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partitionKey := request.Entry.Key.Key
	partition := s.PartitionBy([]byte(partitionKey))

	service := protocol.ServiceId{
		Type: Type,
		Name: request.Headers.PrimitiveID,
	}
	output, err := partition.DoCommand(ctx, service, putOp, input)
	if err != nil {
		s.log.Errorf("Request PutRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &_map.PutResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request PutRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending PutResponse %+v", response)
	return response, nil
}

func (s *Proxy) Get(ctx context.Context, request *_map.GetRequest) (*_map.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partitionKey := request.Key
	partition := s.PartitionBy([]byte(partitionKey))

	service := protocol.ServiceId{
		Type: Type,
		Name: request.Headers.PrimitiveID,
	}
	output, err := partition.DoQuery(ctx, service, getOp, input)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &_map.GetResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Remove(ctx context.Context, request *_map.RemoveRequest) (*_map.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partitionKey := request.Key.Key
	partition := s.PartitionBy([]byte(partitionKey))

	service := protocol.ServiceId{
		Type: Type,
		Name: request.Headers.PrimitiveID,
	}
	output, err := partition.DoCommand(ctx, service, removeOp, input)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &_map.RemoveResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *Proxy) Clear(ctx context.Context, request *_map.ClearRequest) (*_map.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partitions := s.Partitions()

	service := protocol.ServiceId{
		Type: Type,
		Name: request.Headers.PrimitiveID,
	}
	outputs, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		return partitions[i].DoCommand(ctx, service, clearOp, input)
	})
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	responses := make([]_map.ClearResponse, 0, len(outputs))
	for _, output := range outputs {
		var response _map.ClearResponse
		err := proto.Unmarshal(output.([]byte), &response)
		if err != nil {
			s.log.Errorf("Request ClearRequest failed: %v", err)
			return nil, errors.Proto(err)
		}
		responses = append(responses, response)
	}

	response := &responses[0]
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *_map.EventsRequest, srv _map.MapService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
	service := protocol.ServiceId{
		Type: Type,
		Name: request.Headers.PrimitiveID,
	}
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoCommandStream(srv.Context(), service, eventsOp, input, stream)
	})
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

		response := &_map.EventsResponse{}
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

func (s *Proxy) Entries(request *_map.EntriesRequest, srv _map.MapService_EntriesServer) error {
	s.log.Debugf("Received EntriesRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request EntriesRequest failed: %v", err)
		return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
	service := protocol.ServiceId{
		Type: Type,
		Name: request.Headers.PrimitiveID,
	}
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoQueryStream(srv.Context(), service, entriesOp, input, stream)
	})
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

		response := &_map.EntriesResponse{}
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
