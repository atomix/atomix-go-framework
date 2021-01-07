package election

import (
	"context"
	election "github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

const Type = "Election"

const (
	enterOp    = "Enter"
	withdrawOp = "Withdraw"
	anointOp   = "Anoint"
	promoteOp  = "Promote"
	evictOp    = "Evict"
	getTermOp  = "GetTerm"
	eventsOp   = "Events"
	snapshotOp = "Snapshot"
	restoreOp  = "Restore"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *rsm.Node) {
	node.RegisterServer(Type, func(server *grpc.Server, client *rsm.Client) {
		election.RegisterLeaderElectionServiceServer(server, &Proxy{
			Proxy: rsm.NewProxy(client),
			log:   logging.GetLogger("atomix", "election"),
		})
	})
}

type Proxy struct {
	*rsm.Proxy
	log logging.Logger
}

func (s *Proxy) Enter(ctx context.Context, request *election.EnterRequest) (*election.EnterResponse, error) {
	s.log.Debugf("Received EnterRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request EnterRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, enterOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request EnterRequest failed: %v", err)
		return nil, err
	}

	response := &election.EnterResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request EnterRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending EnterResponse %+v", response)
	return response, nil
}

func (s *Proxy) Withdraw(ctx context.Context, request *election.WithdrawRequest) (*election.WithdrawResponse, error) {
	s.log.Debugf("Received WithdrawRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request WithdrawRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, withdrawOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request WithdrawRequest failed: %v", err)
		return nil, err
	}

	response := &election.WithdrawResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request WithdrawRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending WithdrawResponse %+v", response)
	return response, nil
}

func (s *Proxy) Anoint(ctx context.Context, request *election.AnointRequest) (*election.AnointResponse, error) {
	s.log.Debugf("Received AnointRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request AnointRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, anointOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request AnointRequest failed: %v", err)
		return nil, err
	}

	response := &election.AnointResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request AnointRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending AnointResponse %+v", response)
	return response, nil
}

func (s *Proxy) Promote(ctx context.Context, request *election.PromoteRequest) (*election.PromoteResponse, error) {
	s.log.Debugf("Received PromoteRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request PromoteRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, promoteOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request PromoteRequest failed: %v", err)
		return nil, err
	}

	response := &election.PromoteResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request PromoteRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending PromoteResponse %+v", response)
	return response, nil
}

func (s *Proxy) Evict(ctx context.Context, request *election.EvictRequest) (*election.EvictResponse, error) {
	s.log.Debugf("Received EvictRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request EvictRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, evictOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request EvictRequest failed: %v", err)
		return nil, err
	}

	response := &election.EvictResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request EvictRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending EvictResponse %+v", response)
	return response, nil
}

func (s *Proxy) GetTerm(ctx context.Context, request *election.GetTermRequest) (*election.GetTermResponse, error) {
	s.log.Debugf("Received GetTermRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request GetTermRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoQuery(ctx, getTermOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request GetTermRequest failed: %v", err)
		return nil, err
	}

	response := &election.GetTermResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request GetTermRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending GetTermResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *election.EventsRequest, srv election.LeaderElectionService_EventsServer) error {
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
		response := &election.EventsResponse{
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

func (s *Proxy) Snapshot(ctx context.Context, request *election.SnapshotRequest) (*election.SnapshotResponse, error) {
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

	response := &election.SnapshotResponse{}
	output := &response.Snapshot
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending SnapshotResponse %+v", response)
	return response, nil
}

func (s *Proxy) Restore(ctx context.Context, request *election.RestoreRequest) (*election.RestoreResponse, error) {
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

	response := &election.RestoreResponse{}
	s.log.Debugf("Sending RestoreResponse %+v", response)
	return response, nil
}
