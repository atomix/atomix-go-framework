package value

import (
	"context"
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy/gossip"
	"google.golang.org/grpc"
	metadata "google.golang.org/grpc/metadata"
	io "io"
)

const Type = "Value"

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *gossip.Node) {
	node.RegisterProxy(func(server *grpc.Server, client *gossip.Client) {
		value.RegisterValueServiceServer(server, &Proxy{
			Proxy: gossip.NewProxy(client),
			log:   logging.GetLogger("atomix", "value"),
		})
	})
}

type Proxy struct {
	*gossip.Proxy
	log logging.Logger
}

func (s *Proxy) Set(ctx context.Context, request *value.SetRequest) (*value.SetResponse, error) {
	s.log.Debugf("Received SetRequest %+v", request)
	partition, err := s.PartitionFrom(ctx)
	if err != nil {
		return nil, errors.Proto(err)
	}

	conn, err := partition.Connect()
	if err != nil {
		return nil, errors.Proto(err)
	}

	client := value.NewValueServiceClient(conn)

	outMD, _ := metadata.FromIncomingContext(ctx)
	s.AddOutgoingMD(outMD)
	partition.AddOutgoingMD(outMD)
	partition.AddOutgoingMD(outMD)
	ctx = metadata.NewOutgoingContext(ctx, outMD)

	var inMD metadata.MD
	response, err := client.Set(ctx, request, grpc.Trailer(&inMD))
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	err = s.HandleIncomingMD(inMD)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Get(ctx context.Context, request *value.GetRequest) (*value.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	partition, err := s.PartitionFrom(ctx)
	if err != nil {
		return nil, errors.Proto(err)
	}

	conn, err := partition.Connect()
	if err != nil {
		return nil, errors.Proto(err)
	}

	client := value.NewValueServiceClient(conn)

	outMD, _ := metadata.FromIncomingContext(ctx)
	s.AddOutgoingMD(outMD)
	partition.AddOutgoingMD(outMD)
	partition.AddOutgoingMD(outMD)
	ctx = metadata.NewOutgoingContext(ctx, outMD)

	var inMD metadata.MD
	response, err := client.Get(ctx, request, grpc.Trailer(&inMD))
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	err = s.HandleIncomingMD(inMD)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *value.EventsRequest, srv value.ValueService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)
	partition, err := s.PartitionFrom(srv.Context())
	if err != nil {
		return errors.Proto(err)
	}

	conn, err := partition.Connect()
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	client := value.NewValueServiceClient(conn)

	outMD, _ := metadata.FromIncomingContext(srv.Context())
	s.AddOutgoingMD(outMD)
	partition.AddOutgoingMD(outMD)
	partition.AddOutgoingMD(outMD)
	ctx := metadata.NewOutgoingContext(srv.Context(), outMD)

	var inMD metadata.MD
	stream, err := client.Events(ctx, request, grpc.Trailer(&inMD))
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			s.log.Debugf("Finished EventsRequest %+v", request)
			err = s.HandleIncomingMD(inMD)
			if err != nil {
				s.log.Errorf("Request EventsRequest failed: %v", err)
				return errors.Proto(err)
			}
			return nil
		} else if err != nil {
			s.log.Errorf("Request EventsRequest failed: %v", err)
			return errors.Proto(err)
		}
		s.log.Debugf("Sending EventsResponse %+v", response)
		if err := srv.Send(response); err != nil {
			s.log.Errorf("Response EventsResponse failed: %v", err)
			return err
		}
	}
}
