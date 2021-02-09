package set

import (
	"context"
	set "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	async "github.com/atomix/go-framework/pkg/atomix/util/async"
	"github.com/golang/protobuf/proto"

	streams "github.com/atomix/go-framework/pkg/atomix/stream"
)

const Type = "Set"

const (
	sizeOp     = "Size"
	containsOp = "Contains"
	addOp      = "Add"
	removeOp   = "Remove"
	clearOp    = "Clear"
	eventsOp   = "Events"
	elementsOp = "Elements"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *rsm.Node) {
	node.PrimitiveTypes().RegisterProxyFunc(Type, func() (interface{}, error) {
		return &Proxy{
			Proxy: rsm.NewProxy(node.Client),
			log:   logging.GetLogger("atomix", "set"),
		}, nil
	})
}

type Proxy struct {
	*rsm.Proxy
	log logging.Logger
}

func (s *Proxy) Size(ctx context.Context, request *set.SizeRequest) (*set.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partitions := s.Partitions()

	outputs, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		return partitions[i].DoQuery(ctx, sizeOp, input)
	})
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	responses := make([]set.SizeResponse, 0, len(outputs))
	for _, output := range outputs {
		var response set.SizeResponse
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

func (s *Proxy) Contains(ctx context.Context, request *set.ContainsRequest) (*set.ContainsResponse, error) {
	s.log.Debugf("Received ContainsRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request ContainsRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partitionKey := request.Element.Value
	partition := s.PartitionBy([]byte(partitionKey))

	output, err := partition.DoQuery(ctx, containsOp, input)
	if err != nil {
		s.log.Errorf("Request ContainsRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &set.ContainsResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request ContainsRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending ContainsResponse %+v", response)
	return response, nil
}

func (s *Proxy) Add(ctx context.Context, request *set.AddRequest) (*set.AddResponse, error) {
	s.log.Debugf("Received AddRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request AddRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partitionKey := request.Element.Value
	partition := s.PartitionBy([]byte(partitionKey))

	output, err := partition.DoCommand(ctx, addOp, input)
	if err != nil {
		s.log.Errorf("Request AddRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &set.AddResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request AddRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending AddResponse %+v", response)
	return response, nil
}

func (s *Proxy) Remove(ctx context.Context, request *set.RemoveRequest) (*set.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partitionKey := request.Element.Value
	partition := s.PartitionBy([]byte(partitionKey))

	output, err := partition.DoCommand(ctx, removeOp, input)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &set.RemoveResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *Proxy) Clear(ctx context.Context, request *set.ClearRequest) (*set.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partitions := s.Partitions()

	outputs, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		return partitions[i].DoCommand(ctx, clearOp, input)
	})
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	responses := make([]set.ClearResponse, 0, len(outputs))
	for _, output := range outputs {
		var response set.ClearResponse
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

func (s *Proxy) Events(request *set.EventsRequest, srv set.SetService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoCommandStream(srv.Context(), eventsOp, input, stream)
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

		response := &set.EventsResponse{}
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

func (s *Proxy) Elements(request *set.ElementsRequest, srv set.SetService_ElementsServer) error {
	s.log.Debugf("Received ElementsRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request ElementsRequest failed: %v", err)
		return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoQueryStream(srv.Context(), elementsOp, input, stream)
	})
	if err != nil {
		s.log.Errorf("Request ElementsRequest failed: %v", err)
		return errors.Proto(err)
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request ElementsRequest failed: %v", result.Error)
			return errors.Proto(result.Error)
		}

		response := &set.ElementsResponse{}
		err = proto.Unmarshal(result.Value.([]byte), response)
		if err != nil {
			s.log.Errorf("Request ElementsRequest failed: %v", err)
			return errors.Proto(err)
		}

		s.log.Debugf("Sending ElementsResponse %+v", response)
		if err = srv.Send(response); err != nil {
			s.log.Errorf("Response ElementsResponse failed: %v", err)
			return errors.Proto(err)
		}
	}
	s.log.Debugf("Finished ElementsRequest %+v", request)
	return nil
}
