// Code generated by atomix-go-framework. DO NOT EDIT.
package value

import (
	"context"
	value "github.com/atomix/atomix-api/go/atomix/primitive/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	storage "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
)

const Type = "Value"

const (
	setOp    storage.OperationID = 1
	getOp    storage.OperationID = 2
	eventsOp storage.OperationID = 3
)

var log = logging.GetLogger("atomix", "proxy", "value")

// NewProxyServer creates a new ProxyServer
func NewProxyServer(client *rsm.Client, readSync bool) value.ValueServiceServer {
	return &ProxyServer{
		Client:   client,
		readSync: readSync,
	}
}

type ProxyServer struct {
	*rsm.Client
	readSync bool
	log      logging.Logger
}

func (s *ProxyServer) Set(ctx context.Context, request *value.SetRequest) (*value.SetResponse, error) {
	log.Debugf("Received SetRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.Proto(errors.NewInvalid("missing primitive headers"))
	}

	primitiveName, ok := rsm.GetPrimitiveName(md)
	if !ok {
		return nil, errors.Proto(errors.NewInvalid("missing primitive header"))
	}

	clusterKey, ok := rsm.GetClusterKey(md)
	if !ok {
		clusterKey = fmt.Sprintf("%s.%s", s.Namespace, primitiveName)
	}

	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      primitiveName,
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.MD{})
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoCommand(ctx, setOp, input)
	if err != nil {
		log.Warnf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &value.SetResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) Get(ctx context.Context, request *value.GetRequest) (*value.GetResponse, error) {
	log.Debugf("Received GetRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.Proto(errors.NewInvalid("missing primitive headers"))
	}

	primitiveName, ok := rsm.GetPrimitiveName(md)
	if !ok {
		return nil, errors.Proto(errors.NewInvalid("missing primitive header"))
	}

	clusterKey, ok := rsm.GetClusterKey(md)
	if !ok {
		clusterKey = fmt.Sprintf("%s.%s", s.Namespace, primitiveName)
	}

	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      primitiveName,
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.MD{})
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoQuery(ctx, getOp, input, s.readSync)
	if err != nil {
		log.Warnf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &value.GetResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) Events(request *value.EventsRequest, srv value.ValueService_EventsServer) error {
	log.Debugf("Received EventsRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	ch := make(chan streams.Result)
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.Proto(errors.NewInvalid("missing primitive headers"))
	}

	primitiveName, ok := rsm.GetPrimitiveName(md)
	if !ok {
		return nil, errors.Proto(errors.NewInvalid("missing primitive header"))
	}

	clusterKey, ok := rsm.GetClusterKey(md)
	if !ok {
		clusterKey = fmt.Sprintf("%s.%s", s.Namespace, primitiveName)
	}

	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      primitiveName,
	}
	ctx := metadata.NewOutgoingContext(ctx, metadata.MD{})
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		return err
	}
	err = service.DoCommandStream(ctx, eventsOp, input, streams.NewChannelStream(ch))
	if err != nil {
		log.Warnf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	for result := range ch {
		if result.Failed() {
			if result.Error == context.Canceled {
				break
			}
			log.Warnf("Request EventsRequest failed: %v", result.Error)
			return errors.Proto(result.Error)
		}

		response := &value.EventsResponse{}
		err = proto.Unmarshal(result.Value.([]byte), response)
		if err != nil {
			log.Errorf("Request EventsRequest failed: %v", err)
			return errors.Proto(err)
		}

		log.Debugf("Sending EventsResponse %+v", response)
		if err = srv.Send(response); err != nil {
			log.Warnf("Response EventsResponse failed: %v", err)
			return err
		}
	}
	log.Debugf("Finished EventsRequest %+v", request)
	return nil
}
