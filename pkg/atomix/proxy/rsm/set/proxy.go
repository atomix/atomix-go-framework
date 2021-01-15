package set

import (
	"context"
	set "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"io"
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
	snapshotOp = "Snapshot"
	restoreOp  = "Restore"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *rsm.Node) {
	node.RegisterServer(Type, func(server *grpc.Server, client *rsm.Client) {
		set.RegisterSetServiceServer(server, &Proxy{
			Proxy: rsm.NewProxy(client),
			log:   logging.GetLogger("atomix", "set"),
		})
	})
}

type Proxy struct {
	*rsm.Proxy
	log logging.Logger
}

func (s *Proxy) Size(ctx context.Context, request *set.SizeRequest) (*set.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)

	var err error
	var inputBytes []byte
	partitions := s.Partitions()
	outputsBytes, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		return partitions[i].DoQuery(ctx, sizeOp, inputBytes, request.Header)
	})
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	outputs := make([]set.SizeOutput, 0, len(outputsBytes))
	for _, outputBytes := range outputsBytes {
		output := set.SizeOutput{}
		err = proto.Unmarshal(outputBytes.([]byte), &output)
		if err != nil {
			s.log.Errorf("Request SizeRequest failed: %v", err)
			return nil, errors.Proto(err)
		}
		outputs = append(outputs, output)
	}

	response := &set.SizeResponse{}
	for _, o := range outputs {
		response.Output.Size_ += o.Size_
	}
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

func (s *Proxy) Contains(ctx context.Context, request *set.ContainsRequest) (*set.ContainsResponse, error) {
	s.log.Debugf("Received ContainsRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request ContainsRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partitionKey := request.Input.Element.Value
	partition := s.PartitionBy([]byte(partitionKey))
	outputBytes, err := partition.DoQuery(ctx, containsOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request ContainsRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &set.ContainsResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request ContainsRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending ContainsResponse %+v", response)
	return response, nil
}

func (s *Proxy) Add(ctx context.Context, request *set.AddRequest) (*set.AddResponse, error) {
	s.log.Debugf("Received AddRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request AddRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partitionKey := request.Input.Element.Value
	partition := s.PartitionBy([]byte(partitionKey))
	outputBytes, err := partition.DoCommand(ctx, addOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request AddRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &set.AddResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request AddRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending AddResponse %+v", response)
	return response, nil
}

func (s *Proxy) Remove(ctx context.Context, request *set.RemoveRequest) (*set.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partitionKey := request.Input.Element.Value
	partition := s.PartitionBy([]byte(partitionKey))
	outputBytes, err := partition.DoCommand(ctx, removeOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &set.RemoveResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *Proxy) Clear(ctx context.Context, request *set.ClearRequest) (*set.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)

	var err error
	var inputBytes []byte
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		_, err := partitions[i].DoCommand(ctx, clearOp, inputBytes, request.Header)
		return err
	})
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &set.ClearResponse{}
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *set.EventsRequest, srv set.SetService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoCommandStream(srv.Context(), eventsOp, inputBytes, request.Header, stream)
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

		sessionOutput := result.Value.(rsm.SessionOutput)
		response := &set.EventsResponse{
			Header: sessionOutput.Header,
		}
		outputBytes := sessionOutput.Value.([]byte)
		output := &response.Output
		err = proto.Unmarshal(outputBytes, output)
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

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request ElementsRequest failed: %v", err)
		return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoQueryStream(srv.Context(), elementsOp, inputBytes, request.Header, stream)
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

		sessionOutput := result.Value.(rsm.SessionOutput)
		response := &set.ElementsResponse{
			Header: sessionOutput.Header,
		}
		outputBytes := sessionOutput.Value.([]byte)
		output := &response.Output
		err = proto.Unmarshal(outputBytes, output)
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

func (s *Proxy) Snapshot(request *set.SnapshotRequest, srv set.SetService_SnapshotServer) error {
	s.log.Debugf("Received SnapshotRequest %+v", request)

	var err error
	var inputBytes []byte

	stream := streams.NewBufferedStream()
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoCommandStream(srv.Context(), snapshotOp, inputBytes, request.Header, stream)
	})
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return errors.Proto(err)
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request SnapshotRequest failed: %v", result.Error)
			return errors.Proto(result.Error)
		}

		sessionOutput := result.Value.(rsm.SessionOutput)
		response := &set.SnapshotResponse{
			Header: sessionOutput.Header,
		}
		outputBytes := sessionOutput.Value.([]byte)
		output := &response.Entry
		err = proto.Unmarshal(outputBytes, output)
		if err != nil {
			s.log.Errorf("Request SnapshotRequest failed: %v", err)
			return errors.Proto(err)
		}

		s.log.Debugf("Sending SnapshotResponse %+v", response)
		if err = srv.Send(response); err != nil {
			s.log.Errorf("Response SnapshotResponse failed: %v", err)
			return errors.Proto(err)
		}
	}
	s.log.Debugf("Finished SnapshotRequest %+v", request)
	return nil
}

func (s *Proxy) Restore(srv set.SetService_RestoreServer) error {
	response := &set.RestoreResponse{}
	for {
		request, err := srv.Recv()
		if err == io.EOF {
			s.log.Debugf("Sending RestoreResponse %+v", response)
			return srv.SendAndClose(response)
		} else if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return errors.Proto(err)
		}

		s.log.Debugf("Received RestoreRequest %+v", request)
		input := &request.Entry
		inputBytes, err := proto.Marshal(input)
		if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return errors.Proto(err)
		}
		partitions := s.Partitions()
		err = async.IterAsync(len(partitions), func(i int) error {
			_, err := partitions[i].DoCommand(srv.Context(), restoreOp, inputBytes, request.Header)
			return errors.Proto(err)
		})
		if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return errors.Proto(err)
		}
	}
}
