package leader

import (
	"context"
	leader "github.com/atomix/api/go/atomix/primitive/leader"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"google.golang.org/grpc"
	"io"
)

const Type = "LeaderLatch"

const (
	latchOp    = "Latch"
	getOp      = "Get"
	eventsOp   = "Events"
	snapshotOp = "Snapshot"
	restoreOp  = "Restore"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *passthrough.Node) {
	node.RegisterProxy("LeaderLatch", func(server *grpc.Server, client *passthrough.Client) {
		leader.RegisterLeaderLatchServiceServer(server, &Proxy{
			Proxy: passthrough.NewProxy(client),
			log:   logging.GetLogger("atomix", "leaderlatch"),
		})
	})
}

type Proxy struct {
	*passthrough.Proxy
	log logging.Logger
}

func (s *Proxy) Latch(ctx context.Context, request *leader.LatchRequest) (*leader.LatchResponse, error) {
	s.log.Debugf("Received LatchRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := leader.NewLeaderLatchServiceClient(conn)
	response, err := client.Latch(ctx, request)
	if err != nil {
		s.log.Errorf("Request LatchRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending LatchResponse %+v", response)
	return response, nil
}

func (s *Proxy) Get(ctx context.Context, request *leader.GetRequest) (*leader.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := leader.NewLeaderLatchServiceClient(conn)
	response, err := client.Get(ctx, request)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *leader.EventsRequest, srv leader.LeaderLatchService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return err
	}

	client := leader.NewLeaderLatchServiceClient(conn)
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

func (s *Proxy) Snapshot(ctx context.Context, request *leader.SnapshotRequest) (*leader.SnapshotResponse, error) {
	s.log.Debugf("Received SnapshotRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := leader.NewLeaderLatchServiceClient(conn)
	response, err := client.Snapshot(ctx, request)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending SnapshotResponse %+v", response)
	return response, nil
}

func (s *Proxy) Restore(ctx context.Context, request *leader.RestoreRequest) (*leader.RestoreResponse, error) {
	s.log.Debugf("Received RestoreRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := leader.NewLeaderLatchServiceClient(conn)
	response, err := client.Restore(ctx, request)
	if err != nil {
		s.log.Errorf("Request RestoreRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending RestoreResponse %+v", response)
	return response, nil
}
