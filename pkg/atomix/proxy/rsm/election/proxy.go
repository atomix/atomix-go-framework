package election

import (
	"context"
	election "github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
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
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request EnterRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition, err := s.PartitionFrom(ctx)
	if err != nil {
		return nil, errors.Proto(err)
	}

	output, err := partition.DoCommand(ctx, enterOp, input)
	if err != nil {
		s.log.Errorf("Request EnterRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &election.EnterResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request EnterRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending EnterResponse %+v", response)
	return response, nil
}

func (s *Proxy) Withdraw(ctx context.Context, request *election.WithdrawRequest) (*election.WithdrawResponse, error) {
	s.log.Debugf("Received WithdrawRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request WithdrawRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition, err := s.PartitionFrom(ctx)
	if err != nil {
		return nil, errors.Proto(err)
	}

	output, err := partition.DoCommand(ctx, withdrawOp, input)
	if err != nil {
		s.log.Errorf("Request WithdrawRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &election.WithdrawResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request WithdrawRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending WithdrawResponse %+v", response)
	return response, nil
}

func (s *Proxy) Anoint(ctx context.Context, request *election.AnointRequest) (*election.AnointResponse, error) {
	s.log.Debugf("Received AnointRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request AnointRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition, err := s.PartitionFrom(ctx)
	if err != nil {
		return nil, errors.Proto(err)
	}

	output, err := partition.DoCommand(ctx, anointOp, input)
	if err != nil {
		s.log.Errorf("Request AnointRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &election.AnointResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request AnointRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending AnointResponse %+v", response)
	return response, nil
}

func (s *Proxy) Promote(ctx context.Context, request *election.PromoteRequest) (*election.PromoteResponse, error) {
	s.log.Debugf("Received PromoteRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request PromoteRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition, err := s.PartitionFrom(ctx)
	if err != nil {
		return nil, errors.Proto(err)
	}

	output, err := partition.DoCommand(ctx, promoteOp, input)
	if err != nil {
		s.log.Errorf("Request PromoteRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &election.PromoteResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request PromoteRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending PromoteResponse %+v", response)
	return response, nil
}

func (s *Proxy) Evict(ctx context.Context, request *election.EvictRequest) (*election.EvictResponse, error) {
	s.log.Debugf("Received EvictRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request EvictRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition, err := s.PartitionFrom(ctx)
	if err != nil {
		return nil, errors.Proto(err)
	}

	output, err := partition.DoCommand(ctx, evictOp, input)
	if err != nil {
		s.log.Errorf("Request EvictRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &election.EvictResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request EvictRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending EvictResponse %+v", response)
	return response, nil
}

func (s *Proxy) GetTerm(ctx context.Context, request *election.GetTermRequest) (*election.GetTermResponse, error) {
	s.log.Debugf("Received GetTermRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request GetTermRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	partition, err := s.PartitionFrom(ctx)
	if err != nil {
		return nil, errors.Proto(err)
	}

	output, err := partition.DoQuery(ctx, getTermOp, input)
	if err != nil {
		s.log.Errorf("Request GetTermRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &election.GetTermResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		s.log.Errorf("Request GetTermRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending GetTermResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *election.EventsRequest, srv election.LeaderElectionService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
	partition, err := s.PartitionFrom(srv.Context())
	if err != nil {
		return errors.Proto(err)
	}

	err = partition.DoCommandStream(srv.Context(), eventsOp, input, stream)
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

		response := &election.EventsResponse{}
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
