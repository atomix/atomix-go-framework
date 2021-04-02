
package counter

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/driver/proxy/gossip"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
)



// NewProxyServer creates a new ProxyServer
func NewProxyServer(client *gossip.Client) counter.CounterServiceServer {
	return &ProxyServer{
        Client: client,
        log:    logging.GetLogger("atomix", "counter"),
    }
}
type ProxyServer struct {
	*gossip.Client
	log logging.Logger
}

func (s *ProxyServer) Set(ctx context.Context, request *counter.SetRequest) (*counter.SetResponse, error) {
	s.log.Debugf("Received SetRequest %+v", request)
    partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	conn, err := partition.Connect()
	if err != nil {
		return nil, errors.Proto(err)
	}

	client := counter.NewCounterServiceClient(conn)
	s.PrepareRequest(&request.Headers)
	ctx = partition.AddHeaders(ctx)
	response, err := client.Set(ctx, request)
	if err != nil {
        s.log.Errorf("Request SetRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}


func (s *ProxyServer) Get(ctx context.Context, request *counter.GetRequest) (*counter.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
    partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	conn, err := partition.Connect()
	if err != nil {
		return nil, errors.Proto(err)
	}

	client := counter.NewCounterServiceClient(conn)
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


func (s *ProxyServer) Increment(ctx context.Context, request *counter.IncrementRequest) (*counter.IncrementResponse, error) {
	s.log.Debugf("Received IncrementRequest %+v", request)
    partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	conn, err := partition.Connect()
	if err != nil {
		return nil, errors.Proto(err)
	}

	client := counter.NewCounterServiceClient(conn)
	s.PrepareRequest(&request.Headers)
	ctx = partition.AddHeaders(ctx)
	response, err := client.Increment(ctx, request)
	if err != nil {
        s.log.Errorf("Request IncrementRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending IncrementResponse %+v", response)
	return response, nil
}


func (s *ProxyServer) Decrement(ctx context.Context, request *counter.DecrementRequest) (*counter.DecrementResponse, error) {
	s.log.Debugf("Received DecrementRequest %+v", request)
    partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	conn, err := partition.Connect()
	if err != nil {
		return nil, errors.Proto(err)
	}

	client := counter.NewCounterServiceClient(conn)
	s.PrepareRequest(&request.Headers)
	ctx = partition.AddHeaders(ctx)
	response, err := client.Decrement(ctx, request)
	if err != nil {
        s.log.Errorf("Request DecrementRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending DecrementResponse %+v", response)
	return response, nil
}

