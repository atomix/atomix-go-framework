package counter

import (
	"context"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	protocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	"github.com/golang/protobuf/proto"
)

const Type = "Counter"

const (
	setOp       = "Set"
	getOp       = "Get"
	incrementOp = "Increment"
	decrementOp = "Decrement"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *rsm.Node) {
	node.PrimitiveTypes().RegisterProxyFunc(Type, func() (interface{}, error) {
		return &Proxy{
			Proxy: rsm.NewProxy(node.Client),
			log:   logging.GetLogger("atomix", "counter"),
		}, nil
	})
}

type Proxy struct {
	*rsm.Proxy
	log logging.Logger
}

func (s *Proxy) Set(ctx context.Context, request *counter.SetRequest) (*counter.SetResponse, error) {
	s.log.Debugf("Received SetRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition, err := s.PartitionFrom(ctx)
	if err != nil {
		return nil, errors.Proto(err)
	}

	service := protocol.ServiceId{
		Type: Type,
		Name: request.Headers.PrimitiveID,
	}
	output, err := partition.DoCommand(ctx, service, setOp, input)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.SetResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Get(ctx context.Context, request *counter.GetRequest) (*counter.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition, err := s.PartitionFrom(ctx)
	if err != nil {
		return nil, errors.Proto(err)
	}

	service := protocol.ServiceId{
		Type: Type,
		Name: request.Headers.PrimitiveID,
	}
	output, err := partition.DoQuery(ctx, service, getOp, input)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.GetResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Increment(ctx context.Context, request *counter.IncrementRequest) (*counter.IncrementResponse, error) {
	s.log.Debugf("Received IncrementRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request IncrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition, err := s.PartitionFrom(ctx)
	if err != nil {
		return nil, errors.Proto(err)
	}

	service := protocol.ServiceId{
		Type: Type,
		Name: request.Headers.PrimitiveID,
	}
	output, err := partition.DoCommand(ctx, service, incrementOp, input)
	if err != nil {
		s.log.Errorf("Request IncrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.IncrementResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request IncrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending IncrementResponse %+v", response)
	return response, nil
}

func (s *Proxy) Decrement(ctx context.Context, request *counter.DecrementRequest) (*counter.DecrementResponse, error) {
	s.log.Debugf("Received DecrementRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request DecrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition, err := s.PartitionFrom(ctx)
	if err != nil {
		return nil, errors.Proto(err)
	}

	service := protocol.ServiceId{
		Type: Type,
		Name: request.Headers.PrimitiveID,
	}
	output, err := partition.DoCommand(ctx, service, decrementOp, input)
	if err != nil {
		s.log.Errorf("Request DecrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.DecrementResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request DecrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending DecrementResponse %+v", response)
	return response, nil
}
