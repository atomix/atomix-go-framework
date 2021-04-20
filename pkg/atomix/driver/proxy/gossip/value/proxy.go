package value

import (
	"context"
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/driver/proxy/gossip"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	io "io"
)

// NewProxyServer creates a new ProxyServer
func NewProxyServer(client *gossip.Client) value.ValueServiceServer {
	return &ProxyServer{
		Client: client,
		log:    logging.GetLogger("atomix", "value"),
	}
}

type ProxyServer struct {
	*gossip.Client
	log logging.Logger
}

func (s *ProxyServer) Set(ctx context.Context, request *value.SetRequest) (*value.SetResponse, error) {
	s.log.Debugf("Received SetRequest %+v", request)
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	conn, err := partition.Connect()
	if err != nil {
		return nil, errors.Proto(err)
	}

	client := value.NewValueServiceClient(conn)
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

func (s *ProxyServer) Get(ctx context.Context, request *value.GetRequest) (*value.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	conn, err := partition.Connect()
	if err != nil {
		return nil, errors.Proto(err)
	}

	client := value.NewValueServiceClient(conn)
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

func (s *ProxyServer) Events(request *value.EventsRequest, srv value.ValueService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)
	partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	conn, err := partition.Connect()
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	client := value.NewValueServiceClient(conn)
	s.PrepareRequest(&request.Headers)
	ctx := partition.AddHeaders(srv.Context())
	stream, err := client.Events(ctx, request)
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			s.log.Debugf("Finished EventsRequest %+v", request)
			return nil
		} else if err != nil {
			s.log.Errorf("Request EventsRequest failed: %v", err)
			return errors.Proto(err)
		}
		s.PrepareResponse(&response.Headers)
		s.log.Debugf("Sending EventsResponse %+v", response)
		if err := srv.Send(response); err != nil {
			s.log.Errorf("Response EventsResponse failed: %v", err)
			return err
		}
	}
}
