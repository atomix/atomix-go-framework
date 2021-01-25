
package _map

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/proxy/gossip"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	async "github.com/atomix/go-framework/pkg/atomix/util/async"
	
	
	io "io"
	sync "sync"
	
	
	
)

const Type = "Map"


// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *gossip.Node) {
	node.RegisterProxy(func(server *grpc.Server, client *gossip.Client) {
		_map.RegisterMapServiceServer(server, &Proxy{
			Proxy: gossip.NewProxy(client),
			log: logging.GetLogger("atomix", "map"),
		})
	})
}
type Proxy struct {
	*gossip.Proxy
	log logging.Logger
}

func (s *Proxy) Size(ctx context.Context, request *_map.SizeRequest) (*_map.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)
	partitions := s.Partitions()
	responses, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
        partition := partitions[i]
        conn, err := partition.Connect()
        if err != nil {
            return nil, err
        }
        client := _map.NewMapServiceClient(conn)
    	s.PrepareRequest(&request.Headers)
        ctx := partition.AddHeaders(ctx)
		response, err := client.Size(ctx, request)
		if err != nil {
		    return nil, err
		}
    	s.PrepareResponse(&response.Headers)
    	return response, nil
	})
	if err != nil {
        s.log.Errorf("Request SizeRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := responses[0].(*_map.SizeResponse)
    for _, r := range responses {
        response.Size_ += r.(*_map.SizeResponse).Size_
    }
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}


func (s *Proxy) Put(ctx context.Context, request *_map.PutRequest) (*_map.PutResponse, error) {
	s.log.Debugf("Received PutRequest %+v", request)
	partitionKey := request.Entry.Key.Key
	partition := s.PartitionBy([]byte(partitionKey))

	conn, err := partition.Connect()
	if err != nil {
		return nil, errors.Proto(err)
	}

	client := _map.NewMapServiceClient(conn)
	s.PrepareRequest(&request.Headers)
	ctx = partition.AddHeaders(ctx)
	response, err := client.Put(ctx, request)
	if err != nil {
        s.log.Errorf("Request PutRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending PutResponse %+v", response)
	return response, nil
}


func (s *Proxy) Get(ctx context.Context, request *_map.GetRequest) (*_map.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	partitionKey := request.Key
	partition := s.PartitionBy([]byte(partitionKey))

	conn, err := partition.Connect()
	if err != nil {
		return nil, errors.Proto(err)
	}

	client := _map.NewMapServiceClient(conn)
	s.PrepareRequest(&request.Headers)
	ctx = partition.AddHeaders(ctx)
	response, err := client.Get(ctx, request)
	if err != nil {
        s.log.Errorf("Request GetRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}


func (s *Proxy) Remove(ctx context.Context, request *_map.RemoveRequest) (*_map.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)
	partitionKey := request.Key.Key
	partition := s.PartitionBy([]byte(partitionKey))

	conn, err := partition.Connect()
	if err != nil {
		return nil, errors.Proto(err)
	}

	client := _map.NewMapServiceClient(conn)
	s.PrepareRequest(&request.Headers)
	ctx = partition.AddHeaders(ctx)
	response, err := client.Remove(ctx, request)
	if err != nil {
        s.log.Errorf("Request RemoveRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}


func (s *Proxy) Clear(ctx context.Context, request *_map.ClearRequest) (*_map.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)
	partitions := s.Partitions()
	responses, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
        partition := partitions[i]
        conn, err := partition.Connect()
        if err != nil {
            return nil, err
        }
        client := _map.NewMapServiceClient(conn)
    	s.PrepareRequest(&request.Headers)
        ctx := partition.AddHeaders(ctx)
		response, err := client.Clear(ctx, request)
		if err != nil {
		    return nil, err
		}
    	s.PrepareResponse(&response.Headers)
    	return response, nil
	})
	if err != nil {
        s.log.Errorf("Request ClearRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := responses[0].(*_map.ClearResponse)
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
	    s.PrepareRequest(&request.Headers)
        ctx := partition.AddHeaders(srv.Context())
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
        return errors.Proto(err)
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
    	        s.PrepareResponse(&response.Headers)
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
	    s.PrepareRequest(&request.Headers)
        ctx := partition.AddHeaders(srv.Context())
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
        return errors.Proto(err)
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
    	        s.PrepareResponse(&response.Headers)
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

