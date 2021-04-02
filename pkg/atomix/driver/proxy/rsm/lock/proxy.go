
package lock

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm"
	storage "github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/golang/protobuf/proto"
	lock "github.com/atomix/api/go/atomix/primitive/lock"
)

const Type = "Lock"

const (
    lockOp = "Lock"
    unlockOp = "Unlock"
    getLockOp = "GetLock"
)

// NewProxyServer creates a new ProxyServer
func NewProxyServer(client *rsm.Client) lock.LockServiceServer {
	return &ProxyServer{
		Client: client,
		log:    logging.GetLogger("atomix", "counter"),
	}
}

type ProxyServer struct {
	*rsm.Client
	log logging.Logger
}

func (s *ProxyServer) Lock(ctx context.Context, request *lock.LockRequest) (*lock.LockResponse, error) {
	s.log.Debugf("Received LockRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request LockRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoCommand(ctx, service, lockOp, input)
	if err != nil {
        s.log.Errorf("Request LockRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := &lock.LockResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
        s.log.Errorf("Request LockRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending LockResponse %+v", response)
	return response, nil
}


func (s *ProxyServer) Unlock(ctx context.Context, request *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	s.log.Debugf("Received UnlockRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request UnlockRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoCommand(ctx, service, unlockOp, input)
	if err != nil {
        s.log.Errorf("Request UnlockRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := &lock.UnlockResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
        s.log.Errorf("Request UnlockRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending UnlockResponse %+v", response)
	return response, nil
}


func (s *ProxyServer) GetLock(ctx context.Context, request *lock.GetLockRequest) (*lock.GetLockResponse, error) {
	s.log.Debugf("Received GetLockRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request GetLockRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition := s.PartitionBy([]byte(request.Headers.PrimitiveID.String()))

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoQuery(ctx, service, getLockOp, input)
	if err != nil {
        s.log.Errorf("Request GetLockRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := &lock.GetLockResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
        s.log.Errorf("Request GetLockRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending GetLockResponse %+v", response)
	return response, nil
}

