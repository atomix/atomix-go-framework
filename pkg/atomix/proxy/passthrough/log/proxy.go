package log

import (
	"context"
	log "github.com/atomix/api/go/atomix/primitive/log"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
	"google.golang.org/grpc"
	"io"
)

const Type = "Log"

const (
	sizeOp       = "Size"
	existsOp     = "Exists"
	appendOp     = "Append"
	getOp        = "Get"
	firstEntryOp = "FirstEntry"
	lastEntryOp  = "LastEntry"
	prevEntryOp  = "PrevEntry"
	nextEntryOp  = "NextEntry"
	removeOp     = "Remove"
	clearOp      = "Clear"
	eventsOp     = "Events"
	entriesOp    = "Entries"
	snapshotOp   = "Snapshot"
	restoreOp    = "Restore"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *passthrough.Node) {
	node.RegisterProxy(func(server *grpc.Server, client *passthrough.Client) {
		log.RegisterLogServiceServer(server, &Proxy{
			Proxy: passthrough.NewProxy(client),
			log:   logging.GetLogger("atomix", "log"),
		})
	})
}

type Proxy struct {
	*passthrough.Proxy
	log logging.Logger
}

func (s *Proxy) Size(ctx context.Context, request *log.SizeRequest) (*log.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := log.NewLogServiceClient(conn)
	response, err := client.Size(ctx, request)
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

func (s *Proxy) Exists(ctx context.Context, request *log.ExistsRequest) (*log.ExistsResponse, error) {
	s.log.Debugf("Received ExistsRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := log.NewLogServiceClient(conn)
	response, err := client.Exists(ctx, request)
	if err != nil {
		s.log.Errorf("Request ExistsRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending ExistsResponse %+v", response)
	return response, nil
}

func (s *Proxy) Append(ctx context.Context, request *log.AppendRequest) (*log.AppendResponse, error) {
	s.log.Debugf("Received AppendRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := log.NewLogServiceClient(conn)
	response, err := client.Append(ctx, request)
	if err != nil {
		s.log.Errorf("Request AppendRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending AppendResponse %+v", response)
	return response, nil
}

func (s *Proxy) Get(ctx context.Context, request *log.GetRequest) (*log.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := log.NewLogServiceClient(conn)
	response, err := client.Get(ctx, request)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Proxy) FirstEntry(ctx context.Context, request *log.FirstEntryRequest) (*log.FirstEntryResponse, error) {
	s.log.Debugf("Received FirstEntryRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := log.NewLogServiceClient(conn)
	response, err := client.FirstEntry(ctx, request)
	if err != nil {
		s.log.Errorf("Request FirstEntryRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending FirstEntryResponse %+v", response)
	return response, nil
}

func (s *Proxy) LastEntry(ctx context.Context, request *log.LastEntryRequest) (*log.LastEntryResponse, error) {
	s.log.Debugf("Received LastEntryRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := log.NewLogServiceClient(conn)
	response, err := client.LastEntry(ctx, request)
	if err != nil {
		s.log.Errorf("Request LastEntryRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending LastEntryResponse %+v", response)
	return response, nil
}

func (s *Proxy) PrevEntry(ctx context.Context, request *log.PrevEntryRequest) (*log.PrevEntryResponse, error) {
	s.log.Debugf("Received PrevEntryRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := log.NewLogServiceClient(conn)
	response, err := client.PrevEntry(ctx, request)
	if err != nil {
		s.log.Errorf("Request PrevEntryRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending PrevEntryResponse %+v", response)
	return response, nil
}

func (s *Proxy) NextEntry(ctx context.Context, request *log.NextEntryRequest) (*log.NextEntryResponse, error) {
	s.log.Debugf("Received NextEntryRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := log.NewLogServiceClient(conn)
	response, err := client.NextEntry(ctx, request)
	if err != nil {
		s.log.Errorf("Request NextEntryRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending NextEntryResponse %+v", response)
	return response, nil
}

func (s *Proxy) Remove(ctx context.Context, request *log.RemoveRequest) (*log.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := log.NewLogServiceClient(conn)
	response, err := client.Remove(ctx, request)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *Proxy) Clear(ctx context.Context, request *log.ClearRequest) (*log.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := log.NewLogServiceClient(conn)
	response, err := client.Clear(ctx, request)
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *log.EventsRequest, srv log.LogService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return err
	}

	client := log.NewLogServiceClient(conn)
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

func (s *Proxy) Entries(request *log.EntriesRequest, srv log.LogService_EntriesServer) error {
	s.log.Debugf("Received EntriesRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		s.log.Errorf("Request EntriesRequest failed: %v", err)
		return err
	}

	client := log.NewLogServiceClient(conn)
	stream, err := client.Entries(srv.Context(), request)
	if err != nil {
		s.log.Errorf("Request EntriesRequest failed: %v", err)
		return err
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			s.log.Debugf("Finished EntriesRequest %+v", request)
			return nil
		} else if err != nil {
			s.log.Errorf("Request EntriesRequest failed: %v", err)
			return err
		}
		s.log.Debugf("Sending EntriesResponse %+v", response)
		if err := srv.Send(response); err != nil {
			s.log.Errorf("Response EntriesResponse failed: %v", err)
			return err
		}
	}
}

func (s *Proxy) Snapshot(request *log.SnapshotRequest, srv log.LogService_SnapshotServer) error {
	s.log.Debugf("Received SnapshotRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return err
	}

	client := log.NewLogServiceClient(conn)
	stream, err := client.Snapshot(srv.Context(), request)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return err
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			s.log.Debugf("Finished SnapshotRequest %+v", request)
			return nil
		} else if err != nil {
			s.log.Errorf("Request SnapshotRequest failed: %v", err)
			return err
		}
		s.log.Debugf("Sending SnapshotResponse %+v", response)
		if err := srv.Send(response); err != nil {
			s.log.Errorf("Response SnapshotResponse failed: %v", err)
			return err
		}
	}
}

func (s *Proxy) Restore(srv log.LogService_RestoreServer) error {
	var stream log.LogService_RestoreClient
	for {
		request, err := srv.Recv()
		if err == io.EOF {
			if stream == nil {
				return nil
			}

			response, err := stream.CloseAndRecv()
			if err != nil {
				s.log.Errorf("Request RestoreRequest failed: %v", err)
				return err
			}
			s.log.Debugf("Sending RestoreResponse %+v", response)
			return srv.SendAndClose(response)
		} else if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return err
		}

		s.log.Debugf("Received RestoreRequest %+v", request)
		if stream == nil {
			header := request.Header
			partition := s.PartitionFor(header.PrimitiveID)
			conn, err := partition.Connect()
			if err != nil {
				return err
			}
			client := log.NewLogServiceClient(conn)
			stream, err = client.Restore(srv.Context())
			if err != nil {
				return err
			}
		}
		partition := stream

		err = partition.Send(request)
		if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return err
		}
	}
}
