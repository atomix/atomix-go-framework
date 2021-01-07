package list

import (
	"context"
	list "github.com/atomix/api/go/atomix/primitive/list"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"google.golang.org/grpc"
	"io"
)

const Type = "List"

const (
	sizeOp     = "Size"
	containsOp = "Contains"
	appendOp   = "Append"
	insertOp   = "Insert"
	getOp      = "Get"
	setOp      = "Set"
	removeOp   = "Remove"
	clearOp    = "Clear"
	eventsOp   = "Events"
	elementsOp = "Elements"
	snapshotOp = "Snapshot"
	restoreOp  = "Restore"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *passthrough.Node) {
	node.RegisterProxy("List", func(server *grpc.Server, client *passthrough.Client) {
		list.RegisterListServiceServer(server, &Proxy{
			Proxy: passthrough.NewProxy(client),
			log:   logging.GetLogger("atomix", "list"),
		})
	})
}

type Proxy struct {
	*passthrough.Proxy
	log logging.Logger
}

func (s *Proxy) Size(ctx context.Context, request *list.SizeRequest) (*list.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := list.NewListServiceClient(conn)
	response, err := client.Size(ctx, request)
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

func (s *Proxy) Contains(ctx context.Context, request *list.ContainsRequest) (*list.ContainsResponse, error) {
	s.log.Debugf("Received ContainsRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := list.NewListServiceClient(conn)
	response, err := client.Contains(ctx, request)
	if err != nil {
		s.log.Errorf("Request ContainsRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending ContainsResponse %+v", response)
	return response, nil
}

func (s *Proxy) Append(ctx context.Context, request *list.AppendRequest) (*list.AppendResponse, error) {
	s.log.Debugf("Received AppendRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := list.NewListServiceClient(conn)
	response, err := client.Append(ctx, request)
	if err != nil {
		s.log.Errorf("Request AppendRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending AppendResponse %+v", response)
	return response, nil
}

func (s *Proxy) Insert(ctx context.Context, request *list.InsertRequest) (*list.InsertResponse, error) {
	s.log.Debugf("Received InsertRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := list.NewListServiceClient(conn)
	response, err := client.Insert(ctx, request)
	if err != nil {
		s.log.Errorf("Request InsertRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending InsertResponse %+v", response)
	return response, nil
}

func (s *Proxy) Get(ctx context.Context, request *list.GetRequest) (*list.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := list.NewListServiceClient(conn)
	response, err := client.Get(ctx, request)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Set(ctx context.Context, request *list.SetRequest) (*list.SetResponse, error) {
	s.log.Debugf("Received SetRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := list.NewListServiceClient(conn)
	response, err := client.Set(ctx, request)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Remove(ctx context.Context, request *list.RemoveRequest) (*list.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := list.NewListServiceClient(conn)
	response, err := client.Remove(ctx, request)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *Proxy) Clear(ctx context.Context, request *list.ClearRequest) (*list.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := list.NewListServiceClient(conn)
	response, err := client.Clear(ctx, request)
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *list.EventsRequest, srv list.ListService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return err
	}

	client := list.NewListServiceClient(conn)
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

func (s *Proxy) Elements(request *list.ElementsRequest, srv list.ListService_ElementsServer) error {
	s.log.Debugf("Received ElementsRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		s.log.Errorf("Request ElementsRequest failed: %v", err)
		return err
	}

	client := list.NewListServiceClient(conn)
	stream, err := client.Elements(srv.Context(), request)
	if err != nil {
		s.log.Errorf("Request ElementsRequest failed: %v", err)
		return err
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			s.log.Debugf("Finished ElementsRequest %+v", request)
			return nil
		} else if err != nil {
			s.log.Errorf("Request ElementsRequest failed: %v", err)
			return err
		}
		s.log.Debugf("Sending ElementsResponse %+v", response)
		if err := srv.Send(response); err != nil {
			s.log.Errorf("Response ElementsResponse failed: %v", err)
			return err
		}
	}
}

func (s *Proxy) Snapshot(request *list.SnapshotRequest, srv list.ListService_SnapshotServer) error {
	s.log.Debugf("Received SnapshotRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return err
	}

	client := list.NewListServiceClient(conn)
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

func (s *Proxy) Restore(srv list.ListService_RestoreServer) error {
	var stream list.ListService_RestoreClient
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
			client := list.NewListServiceClient(conn)
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
