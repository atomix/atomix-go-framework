package _map

import (
	"context"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy/p2p"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc"
	"io"
	"sync"
)

const Type = "Map"

const (
	sizeOp     = "Size"
	existsOp   = "Exists"
	putOp      = "Put"
	getOp      = "Get"
	removeOp   = "Remove"
	clearOp    = "Clear"
	eventsOp   = "Events"
	entriesOp  = "Entries"
	snapshotOp = "Snapshot"
	restoreOp  = "Restore"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *p2p.Node) {
	node.RegisterProxy(func(server *grpc.Server, client *p2p.Client) {
		_map.RegisterMapServiceServer(server, &Proxy{
			Proxy: p2p.NewProxy(client),
			log:   logging.GetLogger("atomix", "map"),
		})
	})
}

type Proxy struct {
	*p2p.Proxy
	log logging.Logger
}

func (s *Proxy) Size(ctx context.Context, request *_map.SizeRequest) (*_map.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)
	partitions := s.Partitions()
	responses, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		partition := partitions[i]
		conn, err := partition.Connect()
		if err != nil {
			s.log.Errorf("Request SizeRequest failed: %v", err)
			return nil, err
		}
		client := _map.NewMapServiceClient(conn)
		ctx = partition.AddHeader(ctx)
		return client.Size(ctx, request)
	})
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, err
	}

	response := &_map.SizeResponse{}
	for _, r := range responses {
		response.Output.Size_ += r.(*_map.SizeResponse).Output.Size_
	}
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

func (s *Proxy) Exists(ctx context.Context, request *_map.ExistsRequest) (*_map.ExistsResponse, error) {
	s.log.Debugf("Received ExistsRequest %+v", request)
	partitionKey := request.Input.Key
	partition := s.PartitionBy([]byte(partitionKey))

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := _map.NewMapServiceClient(conn)
	ctx = partition.AddHeader(ctx)
	response, err := client.Exists(ctx, request)
	if err != nil {
		s.log.Errorf("Request ExistsRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending ExistsResponse %+v", response)
	return response, nil
}

func (s *Proxy) Put(ctx context.Context, request *_map.PutRequest) (*_map.PutResponse, error) {
	s.log.Debugf("Received PutRequest %+v", request)
	partitionKey := request.Input.Key
	partition := s.PartitionBy([]byte(partitionKey))

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := _map.NewMapServiceClient(conn)
	ctx = partition.AddHeader(ctx)
	response, err := client.Put(ctx, request)
	if err != nil {
		s.log.Errorf("Request PutRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending PutResponse %+v", response)
	return response, nil
}

func (s *Proxy) Get(ctx context.Context, request *_map.GetRequest) (*_map.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	partitionKey := request.Input.Key
	partition := s.PartitionBy([]byte(partitionKey))

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := _map.NewMapServiceClient(conn)
	ctx = partition.AddHeader(ctx)
	response, err := client.Get(ctx, request)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Remove(ctx context.Context, request *_map.RemoveRequest) (*_map.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)
	partitionKey := request.Input.Key
	partition := s.PartitionBy([]byte(partitionKey))

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := _map.NewMapServiceClient(conn)
	ctx = partition.AddHeader(ctx)
	response, err := client.Remove(ctx, request)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *Proxy) Clear(ctx context.Context, request *_map.ClearRequest) (*_map.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		conn, err := partition.Connect()
		if err != nil {
			s.log.Errorf("Request ClearRequest failed: %v", err)
			return err
		}
		client := _map.NewMapServiceClient(conn)
		ctx = partition.AddHeader(ctx)
		_, err = client.Clear(ctx, request)
		return err
	})
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, err
	}

	response := &_map.ClearResponse{}
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *_map.EventsRequest, srv _map.MapService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)
	partitions := s.Partitions()
	wg := &sync.WaitGroup{}
	responseCh := make(chan *_map.EventsResponse)
	errCh := make(chan error)
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		conn, err := partition.Connect()
		if err != nil {
			s.log.Errorf("Request EventsRequest failed: %v", err)
			return err
		}
		client := _map.NewMapServiceClient(conn)
		ctx := partition.AddHeader(srv.Context())
		stream, err := client.Events(ctx, request)
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

func (s *Proxy) Entries(request *_map.EntriesRequest, srv _map.MapService_EntriesServer) error {
	s.log.Debugf("Received EntriesRequest %+v", request)
	partitions := s.Partitions()
	wg := &sync.WaitGroup{}
	responseCh := make(chan *_map.EntriesResponse)
	errCh := make(chan error)
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		conn, err := partition.Connect()
		if err != nil {
			s.log.Errorf("Request EntriesRequest failed: %v", err)
			return err
		}
		client := _map.NewMapServiceClient(conn)
		ctx := partition.AddHeader(srv.Context())
		stream, err := client.Entries(ctx, request)
		if err != nil {
			s.log.Errorf("Request EntriesRequest failed: %v", err)
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
		s.log.Errorf("Request EntriesRequest failed: %v", err)
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
				s.log.Debugf("Sending EntriesResponse %+v", response)
				err := srv.Send(response)
				if err != nil {
					s.log.Errorf("Response EntriesResponse failed: %v", err)
					return err
				}
			}
		case err := <-errCh:
			if err != nil {
				s.log.Errorf("Request EntriesRequest failed: %v", err)
			}
			s.log.Debugf("Finished EntriesRequest %+v", request)
			return err
		}
	}
}

func (s *Proxy) Snapshot(request *_map.SnapshotRequest, srv _map.MapService_SnapshotServer) error {
	s.log.Debugf("Received SnapshotRequest %+v", request)
	partitions := s.Partitions()
	wg := &sync.WaitGroup{}
	responseCh := make(chan *_map.SnapshotResponse)
	errCh := make(chan error)
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		conn, err := partition.Connect()
		if err != nil {
			s.log.Errorf("Request SnapshotRequest failed: %v", err)
			return err
		}
		client := _map.NewMapServiceClient(conn)
		ctx := partition.AddHeader(srv.Context())
		stream, err := client.Snapshot(ctx, request)
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

func (s *Proxy) Restore(srv _map.MapService_RestoreServer) error {
	var streams map[p2p.PartitionID]_map.MapService_RestoreClient
	for {
		request, err := srv.Recv()
		if err == io.EOF {
			if streams == nil {
				return nil
			}

			responses := make([]*_map.RestoreResponse, 0, len(streams))
			for _, stream := range streams {
				response, err := stream.CloseAndRecv()
				if err != nil {
					s.log.Errorf("Request RestoreRequest failed: %v", err)
					return err
				}
				responses = append(responses, response)
			}
			clients := make([]_map.MapService_RestoreClient, 0, len(streams))
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

			response := &_map.RestoreResponse{}
			s.log.Debugf("Sending RestoreResponse %+v", response)
			return srv.SendAndClose(response)
		} else if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return err
		}

		s.log.Debugf("Received RestoreRequest %+v", request)
		if streams == nil {
			partitions := s.Partitions()
			streams = make(map[p2p.PartitionID]_map.MapService_RestoreClient)
			for _, partition := range partitions {
				conn, err := partition.Connect()
				if err != nil {
					return err
				}
				client := _map.NewMapServiceClient(conn)
				ctx := partition.AddHeader(srv.Context())
				stream, err := client.Restore(ctx)
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
