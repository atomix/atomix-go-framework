package value

import (
	"context"
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

const Type = "Value"

const (
	setOp      = "Set"
	getOp      = "Get"
	eventsOp   = "Events"
	snapshotOp = "Snapshot"
	restoreOp  = "Restore"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *rsm.Node) {
	node.RegisterServer(Type, func(server *grpc.Server, client *rsm.Client) {
		value.RegisterValueServiceServer(server, &Proxy{
			Proxy: rsm.NewProxy(client),
			log:   logging.GetLogger("atomix", "value"),
		})
	})
}

type Proxy struct {
	*rsm.Proxy
	log logging.Logger
}

func (s *Proxy) Set(ctx context.Context, request *value.SetRequest) (*value.SetResponse, error) {
	s.log.Debugf("Received SetRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, setOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, err
	}

	response := &value.SetResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Get(ctx context.Context, request *value.GetRequest) (*value.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoQuery(ctx, getOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, err
	}

	response := &value.GetResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *value.EventsRequest, srv value.ValueService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return err
	}

	stream := streams.NewBufferedStream()
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	err = partition.DoCommandStream(srv.Context(), eventsOp, inputBytes, request.Header, stream)
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return err
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request EventsRequest failed: %v", result.Error)
			return result.Error
		}

		sessionOutput := result.Value.(rsm.SessionOutput)
		response := &value.EventsResponse{
			Header: sessionOutput.Header,
		}
		outputBytes := sessionOutput.Value.([]byte)
		output := &response.Output
		err = proto.Unmarshal(outputBytes, output)
		if err != nil {
			s.log.Errorf("Request EventsRequest failed: %v", err)
			return err
		}

		s.log.Debugf("Sending EventsResponse %+v", response)
		if err = srv.Send(response); err != nil {
			s.log.Errorf("Response EventsResponse failed: %v", err)
			return err
		}
	}
	s.log.Debugf("Finished EventsRequest %+v", request)
	return nil
}

func (s *Proxy) Snapshot(ctx context.Context, request *value.SnapshotRequest) (*value.SnapshotResponse, error) {
	s.log.Debugf("Received SnapshotRequest %+v", request)

	var err error
	var inputBytes []byte
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, snapshotOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return nil, err
	}

	response := &value.SnapshotResponse{}
	output := &response.Snapshot
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending SnapshotResponse %+v", response)
	return response, nil
}

func (s *Proxy) Restore(ctx context.Context, request *value.RestoreRequest) (*value.RestoreResponse, error) {
	s.log.Debugf("Received RestoreRequest %+v", request)

	var err error
	input := &request.Snapshot
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request RestoreRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	_, err = partition.DoCommand(ctx, restoreOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request RestoreRequest failed: %v", err)
		return nil, err
	}

	response := &value.RestoreResponse{}
	s.log.Debugf("Sending RestoreResponse %+v", response)
	return response, nil
}
