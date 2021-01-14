package set

import (
	"context"
	set "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc"
	"io"
	"sync"
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
func RegisterProxy(node *passthrough.Node) {
	node.RegisterProxy(func(server *grpc.Server, client *passthrough.Client) {
		set.RegisterSetServiceServer(server, &Proxy{
			Proxy: passthrough.NewProxy(client),
			log:   logging.GetLogger("atomix", "set"),
		})
	})
}

type Proxy struct {
	*passthrough.Proxy
	log logging.Logger
}

func (s *Proxy) Size(ctx context.Context, request *set.SizeRequest) (*set.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)
	partitions := s.Partitions()
	responses, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		conn, err := partitions[i].Connect()
		if err != nil {
			s.log.Errorf("Request SizeRequest failed: %v", err)
			return nil, err
		}
		client := set.NewSetServiceClient(conn)
		return client.Size(ctx, request)
	})
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, err
	}

	response := &set.SizeResponse{}
	for _, r := range responses {
		response.Output.Size_ += r.(*set.SizeResponse).Output.Size_
	}
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

func (s *Proxy) Contains(ctx context.Context, request *set.ContainsRequest) (*set.ContainsResponse, error) {
	s.log.Debugf("Received ContainsRequest %+v", request)
	partitionKey := request.Input.Value
	partition := s.PartitionBy([]byte(partitionKey))

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := set.NewSetServiceClient(conn)
	response, err := client.Contains(ctx, request)
	if err != nil {
		s.log.Errorf("Request ContainsRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending ContainsResponse %+v", response)
	return response, nil
}

func (s *Proxy) Add(ctx context.Context, request *set.AddRequest) (*set.AddResponse, error) {
	s.log.Debugf("Received AddRequest %+v", request)
	partitionKey := request.Input.Value
	partition := s.PartitionBy([]byte(partitionKey))

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := set.NewSetServiceClient(conn)
	response, err := client.Add(ctx, request)
	if err != nil {
		s.log.Errorf("Request AddRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending AddResponse %+v", response)
	return response, nil
}

func (s *Proxy) Remove(ctx context.Context, request *set.RemoveRequest) (*set.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)
	partitionKey := request.Input.Value
	partition := s.PartitionBy([]byte(partitionKey))

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := set.NewSetServiceClient(conn)
	response, err := client.Remove(ctx, request)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *Proxy) Clear(ctx context.Context, request *set.ClearRequest) (*set.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		conn, err := partitions[i].Connect()
		if err != nil {
			s.log.Errorf("Request ClearRequest failed: %v", err)
			return err
		}
		client := set.NewSetServiceClient(conn)
		_, err = client.Clear(ctx, request)
		return err
	})
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, err
	}

	response := &set.ClearResponse{}
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *set.EventsRequest, srv set.SetService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)
	partitions := s.Partitions()
	wg := &sync.WaitGroup{}
	responseCh := make(chan *set.EventsResponse)
	errCh := make(chan error)
	err := async.IterAsync(len(partitions), func(i int) error {
		conn, err := partitions[i].Connect()
		if err != nil {
			s.log.Errorf("Request EventsRequest failed: %v", err)
			return err
		}
		client := set.NewSetServiceClient(conn)
		stream, err := client.Events(srv.Context(), request)
		if err != nil {
			s.log.Errorf("Request EventsRequest failed: %v", err)
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				response, err := stream.Recv()
				if err == io.EOF {
					return
				} else if err != nil {
					errCh <- err
				} else {
					responseCh <- response
				}
			}
		}()
		return nil
	})
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return err
	}

	go func() {
		wg.Wait()
		close(responseCh)
		close(errCh)
	}()

	for {
		select {
		case response, ok := <-responseCh:
			if ok {
				s.log.Debugf("Sending EventsResponse %+v", response)
				err := srv.Send(response)
				if err != nil {
					s.log.Errorf("Response EventsResponse failed: %v", err)
					return err
				}
			}
		case err := <-errCh:
			if err != nil {
				s.log.Errorf("Request EventsRequest failed: %v", err)
			}
			s.log.Debugf("Finished EventsRequest %+v", request)
			return err
		}
	}
}

func (s *Proxy) Elements(request *set.ElementsRequest, srv set.SetService_ElementsServer) error {
	s.log.Debugf("Received ElementsRequest %+v", request)
	partitions := s.Partitions()
	wg := &sync.WaitGroup{}
	responseCh := make(chan *set.ElementsResponse)
	errCh := make(chan error)
	err := async.IterAsync(len(partitions), func(i int) error {
		conn, err := partitions[i].Connect()
		if err != nil {
			s.log.Errorf("Request ElementsRequest failed: %v", err)
			return err
		}
		client := set.NewSetServiceClient(conn)
		stream, err := client.Elements(srv.Context(), request)
		if err != nil {
			s.log.Errorf("Request ElementsRequest failed: %v", err)
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				response, err := stream.Recv()
				if err == io.EOF {
					return
				} else if err != nil {
					errCh <- err
				} else {
					responseCh <- response
				}
			}
		}()
		return nil
	})
	if err != nil {
		s.log.Errorf("Request ElementsRequest failed: %v", err)
		return err
	}

	go func() {
		wg.Wait()
		close(responseCh)
		close(errCh)
	}()

	for {
		select {
		case response, ok := <-responseCh:
			if ok {
				s.log.Debugf("Sending ElementsResponse %+v", response)
				err := srv.Send(response)
				if err != nil {
					s.log.Errorf("Response ElementsResponse failed: %v", err)
					return err
				}
			}
		case err := <-errCh:
			if err != nil {
				s.log.Errorf("Request ElementsRequest failed: %v", err)
			}
			s.log.Debugf("Finished ElementsRequest %+v", request)
			return err
		}
	}
}

func (s *Proxy) Snapshot(request *set.SnapshotRequest, srv set.SetService_SnapshotServer) error {
	s.log.Debugf("Received SnapshotRequest %+v", request)
	partitions := s.Partitions()
	wg := &sync.WaitGroup{}
	responseCh := make(chan *set.SnapshotResponse)
	errCh := make(chan error)
	err := async.IterAsync(len(partitions), func(i int) error {
		conn, err := partitions[i].Connect()
		if err != nil {
			s.log.Errorf("Request SnapshotRequest failed: %v", err)
			return err
		}
		client := set.NewSetServiceClient(conn)
		stream, err := client.Snapshot(srv.Context(), request)
		if err != nil {
			s.log.Errorf("Request SnapshotRequest failed: %v", err)
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				response, err := stream.Recv()
				if err == io.EOF {
					return
				} else if err != nil {
					errCh <- err
				} else {
					responseCh <- response
				}
			}
		}()
		return nil
	})
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return err
	}

	go func() {
		wg.Wait()
		close(responseCh)
		close(errCh)
	}()

	for {
		select {
		case response, ok := <-responseCh:
			if ok {
				s.log.Debugf("Sending SnapshotResponse %+v", response)
				err := srv.Send(response)
				if err != nil {
					s.log.Errorf("Response SnapshotResponse failed: %v", err)
					return err
				}
			}
		case err := <-errCh:
			if err != nil {
				s.log.Errorf("Request SnapshotRequest failed: %v", err)
			}
			s.log.Debugf("Finished SnapshotRequest %+v", request)
			return err
		}
	}
}

func (s *Proxy) Restore(srv set.SetService_RestoreServer) error {
	var streams map[passthrough.PartitionID]set.SetService_RestoreClient
	for {
		request, err := srv.Recv()
		if err == io.EOF {
			if streams == nil {
				return nil
			}

			responses := make([]*set.RestoreResponse, 0, len(streams))
			for _, stream := range streams {
				response, err := stream.CloseAndRecv()
				if err != nil {
					s.log.Errorf("Request RestoreRequest failed: %v", err)
					return err
				}
				responses = append(responses, response)
			}
			clients := make([]set.SetService_RestoreClient, 0, len(streams))
			for _, stream := range streams {
				clients = append(clients, stream)
			}
			err := async.IterAsync(len(clients), func(i int) error {
				_, err = clients[i].CloseAndRecv()
				return err
			})
			if err != nil {
				s.log.Errorf("Request RestoreRequest failed: %v", err)
				return err
			}

			response := &set.RestoreResponse{}
			s.log.Debugf("Sending RestoreResponse %+v", response)
			return srv.SendAndClose(response)
		} else if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return err
		}

		s.log.Debugf("Received RestoreRequest %+v", request)
		if streams == nil {
			partitions := s.Partitions()
			streams = make(map[passthrough.PartitionID]set.SetService_RestoreClient)
			for _, partition := range partitions {
				conn, err := partition.Connect()
				if err != nil {
					return err
				}
				client := set.NewSetServiceClient(conn)
				stream, err := client.Restore(srv.Context())
				if err != nil {
					return err
				}
				streams[partition.ID] = stream
			}
		}
		for _, stream := range streams {
			err := stream.Send(request)
			if err != nil {
				s.log.Errorf("Request RestoreRequest failed: %v", err)
				return err
			}
		}
	}
}
