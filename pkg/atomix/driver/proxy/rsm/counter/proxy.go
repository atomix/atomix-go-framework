// Code generated by atomix-go-framework. DO NOT EDIT.
package counter

import (
	"context"
	counter "github.com/atomix/atomix-api/go/atomix/primitive/counter"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	storage "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/golang/protobuf/proto"
)

const Type = "Counter"

const (
	setOp       storage.OperationID = 1
	getOp       storage.OperationID = 2
	incrementOp storage.OperationID = 3
	decrementOp storage.OperationID = 4
)

var log = logging.GetLogger("atomix", "proxy", "counter")

// NewProxyServer creates a new ProxyServer
func NewProxyServer(client *rsm.Client, readSync bool) counter.CounterServiceServer {
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

func (s *ProxyServer) Set(ctx context.Context, request *counter.SetRequest) (*counter.SetResponse, error) {
	log.Debugf("Received SetRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoCommand(ctx, setOp, input)
	if err != nil {
		log.Debugf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.SetResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) Get(ctx context.Context, request *counter.GetRequest) (*counter.GetResponse, error) {
	log.Debugf("Received GetRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoQuery(ctx, getOp, input, s.readSync)
	if err != nil {
		log.Debugf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.GetResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) Increment(ctx context.Context, request *counter.IncrementRequest) (*counter.IncrementResponse, error) {
	log.Debugf("Received IncrementRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request IncrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request IncrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoCommand(ctx, incrementOp, input)
	if err != nil {
		log.Debugf("Request IncrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.IncrementResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request IncrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending IncrementResponse %+v", response)
	return response, nil
}

func (s *ProxyServer) Decrement(ctx context.Context, request *counter.DecrementRequest) (*counter.DecrementResponse, error) {
	log.Debugf("Received DecrementRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
		log.Errorf("Request DecrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	clusterKey := request.Headers.ClusterKey
	if clusterKey == "" {
		clusterKey = request.Headers.PrimitiveID.String()
	}
	partition := s.PartitionBy([]byte(clusterKey))

	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	service, err := partition.GetService(ctx, serviceInfo)
	if err != nil {
		log.Errorf("Request DecrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	output, err := service.DoCommand(ctx, decrementOp, input)
	if err != nil {
		log.Debugf("Request DecrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &counter.DecrementResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
		log.Errorf("Request DecrementRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	log.Debugf("Sending DecrementResponse %+v", response)
	return response, nil
}
