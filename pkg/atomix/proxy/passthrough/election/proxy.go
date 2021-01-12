package election

import (
	"context"
	election "github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
	"google.golang.org/grpc"
	"io"
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
func RegisterProxy(node *passthrough.Node) {
	node.RegisterProxy("Election", func(server *grpc.Server, client *passthrough.Client) {
		election.RegisterLeaderElectionServiceServer(server, &Proxy{
			Proxy: passthrough.NewProxy(client),
			log:   logging.GetLogger("atomix", "election"),
		})
	})
}

type Proxy struct {
	*passthrough.Proxy
	log logging.Logger
}

func (s *Proxy) Enter(ctx context.Context, request *election.EnterRequest) (*election.EnterResponse, error) {
	s.log.Debugf("Received EnterRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := election.NewLeaderElectionServiceClient(conn)
	response, err := client.Enter(ctx, request)
	if err != nil {
		s.log.Errorf("Request EnterRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending EnterResponse %+v", response)
	return response, nil
}

func (s *Proxy) Withdraw(ctx context.Context, request *election.WithdrawRequest) (*election.WithdrawResponse, error) {
	s.log.Debugf("Received WithdrawRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := election.NewLeaderElectionServiceClient(conn)
	response, err := client.Withdraw(ctx, request)
	if err != nil {
		s.log.Errorf("Request WithdrawRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending WithdrawResponse %+v", response)
	return response, nil
}

func (s *Proxy) Anoint(ctx context.Context, request *election.AnointRequest) (*election.AnointResponse, error) {
	s.log.Debugf("Received AnointRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := election.NewLeaderElectionServiceClient(conn)
	response, err := client.Anoint(ctx, request)
	if err != nil {
		s.log.Errorf("Request AnointRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending AnointResponse %+v", response)
	return response, nil
}

func (s *Proxy) Promote(ctx context.Context, request *election.PromoteRequest) (*election.PromoteResponse, error) {
	s.log.Debugf("Received PromoteRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := election.NewLeaderElectionServiceClient(conn)
	response, err := client.Promote(ctx, request)
	if err != nil {
		s.log.Errorf("Request PromoteRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending PromoteResponse %+v", response)
	return response, nil
}

func (s *Proxy) Evict(ctx context.Context, request *election.EvictRequest) (*election.EvictResponse, error) {
	s.log.Debugf("Received EvictRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := election.NewLeaderElectionServiceClient(conn)
	response, err := client.Evict(ctx, request)
	if err != nil {
		s.log.Errorf("Request EvictRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending EvictResponse %+v", response)
	return response, nil
}

func (s *Proxy) GetTerm(ctx context.Context, request *election.GetTermRequest) (*election.GetTermResponse, error) {
	s.log.Debugf("Received GetTermRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := election.NewLeaderElectionServiceClient(conn)
	response, err := client.GetTerm(ctx, request)
	if err != nil {
		s.log.Errorf("Request GetTermRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending GetTermResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *election.EventsRequest, srv election.LeaderElectionService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return err
	}

	client := election.NewLeaderElectionServiceClient(conn)
	stream, err := client.Events(srv.Context(), request)
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return err
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			s.log.Debugf("Finished EventsRequest %+v", request)
			return nil
		} else if err != nil {
			s.log.Errorf("Request EventsRequest failed: %v", err)
			return err
		}
		s.log.Debugf("Sending EventsResponse %+v", response)
		if err := srv.Send(response); err != nil {
			s.log.Errorf("Response EventsResponse failed: %v", err)
			return err
		}
	}
}

func (s *Proxy) Snapshot(ctx context.Context, request *election.SnapshotRequest) (*election.SnapshotResponse, error) {
	s.log.Debugf("Received SnapshotRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := election.NewLeaderElectionServiceClient(conn)
	response, err := client.Snapshot(ctx, request)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending SnapshotResponse %+v", response)
	return response, nil
}

func (s *Proxy) Restore(ctx context.Context, request *election.RestoreRequest) (*election.RestoreResponse, error) {
	s.log.Debugf("Received RestoreRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := election.NewLeaderElectionServiceClient(conn)
	response, err := client.Restore(ctx, request)
	if err != nil {
		s.log.Errorf("Request RestoreRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending RestoreResponse %+v", response)
	return response, nil
}
