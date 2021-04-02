
package leader

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm"
	storage "github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/golang/protobuf/proto"
	leader "github.com/atomix/api/go/atomix/primitive/leader"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
)

const Type = "LeaderLatch"

const (
    latchOp = "Latch"
    getOp = "Get"
    eventsOp = "Events"
)

// NewProxyServer creates a new ProxyServer
func NewProxyServer(client *rsm.Client) leader.LeaderLatchServiceServer {
	return &ProxyServer{
		Client: client,
		log:    logging.GetLogger("atomix", "counter"),
	}
}

type ProxyServer struct {
	*rsm.Client
	log logging.Logger
}

func (s *ProxyServer) Latch(ctx context.Context, request *leader.LatchRequest) (*leader.LatchResponse, error) {
	s.log.Debugf("Received LatchRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request LatchRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoCommand(ctx, service, latchOp, input)
	if err != nil {
        s.log.Errorf("Request LatchRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := &leader.LatchResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
        s.log.Errorf("Request LatchRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending LatchResponse %+v", response)
	return response, nil
}


func (s *ProxyServer) Get(ctx context.Context, request *leader.GetRequest) (*leader.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request GetRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoQuery(ctx, service, getOp, input)
	if err != nil {
        s.log.Errorf("Request GetRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := &leader.GetResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
        s.log.Errorf("Request GetRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}


func (s *ProxyServer) Events(request *leader.EventsRequest, srv leader.LeaderLatchService_EventsServer) error {
    s.log.Debugf("Received EventsRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request EventsRequest failed: %v", err)
        return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
    partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	err = partition.DoCommandStream(srv.Context(), service, eventsOp, input, stream)
	if err != nil {
        s.log.Errorf("Request EventsRequest failed: %v", err)
	    return errors.Proto(err)
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request EventsRequest failed: %v", result.Error)
			return errors.Proto(result.Error)
		}

		response := &leader.EventsResponse{}
        err = proto.Unmarshal(result.Value.([]byte), response)
        if err != nil {
            s.log.Errorf("Request EventsRequest failed: %v", err)
            return errors.Proto(err)
        }

		s.log.Debugf("Sending EventsResponse %+v", response)
		if err = srv.Send(response); err != nil {
            s.log.Errorf("Response EventsResponse failed: %v", err)
			return errors.Proto(err)
		}
	}
	s.log.Debugf("Finished EventsRequest %+v", request)
	return nil
}

