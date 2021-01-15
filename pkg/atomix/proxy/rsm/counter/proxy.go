package counter

import (
	"context"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

const Type = "Counter"

const (
	setOp       = "Set"
	getOp       = "Get"
	incrementOp = "Increment"
	decrementOp = "Decrement"
	snapshotOp  = "Snapshot"
	restoreOp   = "Restore"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *rsm.Node) {
	node.RegisterServer(Type, func(server *grpc.Server, client *rsm.Client) {
		counter.RegisterCounterServiceServer(server, &Proxy{
			Proxy: rsm.NewProxy(client),
			log:   logging.GetLogger("atomix", "counter"),
		})
	})
}

type Proxy struct {
	*rsm.Proxy
	log logging.Logger
}

func (s *Proxy) Set(ctx context.Context, request *counter.SetRequest) (*counter.SetResponse, error) {
	s.log.Debugf("Received SetRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, setOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.SetResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Get(ctx context.Context, request *counter.GetRequest) (*counter.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoQuery(ctx, getOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.GetResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Increment(ctx context.Context, request *counter.IncrementRequest) (*counter.IncrementResponse, error) {
	s.log.Debugf("Received IncrementRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request IncrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, incrementOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request IncrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.IncrementResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request IncrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending IncrementResponse %+v", response)
	return response, nil
}

func (s *Proxy) Decrement(ctx context.Context, request *counter.DecrementRequest) (*counter.DecrementResponse, error) {
	s.log.Debugf("Received DecrementRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request DecrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, decrementOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request DecrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.DecrementResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request DecrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending DecrementResponse %+v", response)
	return response, nil
}

func (s *Proxy) Snapshot(ctx context.Context, request *counter.SnapshotRequest) (*counter.SnapshotResponse, error) {
	s.log.Debugf("Received SnapshotRequest %+v", request)

	var err error
	var inputBytes []byte
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, snapshotOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.SnapshotResponse{}
	output := &response.Snapshot
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending SnapshotResponse %+v", response)
	return response, nil
}

func (s *Proxy) Restore(ctx context.Context, request *counter.RestoreRequest) (*counter.RestoreResponse, error) {
	s.log.Debugf("Received RestoreRequest %+v", request)

	var err error
	input := &request.Snapshot
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request RestoreRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	_, err = partition.DoCommand(ctx, restoreOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request RestoreRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.RestoreResponse{}
	s.log.Debugf("Sending RestoreResponse %+v", response)
	return response, nil
}
