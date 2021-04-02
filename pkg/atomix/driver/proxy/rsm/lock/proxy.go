
package lock

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	protocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
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

// NewLockProxyServer creates a new LockProxyServer
func NewLockProxyServer(client *rsm.Client) lock.LockServiceServer {
	return &LockProxyServer{
		Proxy: rsm.NewProxy(client),
		log:   logging.GetLogger("atomix", "counter"),
	}
}

type LockProxyServer struct {
	*rsm.Proxy
	log logging.Logger
}

func (s *LockProxyServer) Lock(ctx context.Context, request *lock.LockRequest) (*lock.LockResponse, error) {
	s.log.Debugf("Received LockRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request LockRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition, err := s.PartitionFrom(ctx)
    if err != nil {
        return nil, errors.Proto(err)
    }

	service := protocol.ServiceId{
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


func (s *LockProxyServer) Unlock(ctx context.Context, request *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	s.log.Debugf("Received UnlockRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request UnlockRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition, err := s.PartitionFrom(ctx)
    if err != nil {
        return nil, errors.Proto(err)
    }

	service := protocol.ServiceId{
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


func (s *LockProxyServer) GetLock(ctx context.Context, request *lock.GetLockRequest) (*lock.GetLockResponse, error) {
	s.log.Debugf("Received GetLockRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request GetLockRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition, err := s.PartitionFrom(ctx)
    if err != nil {
        return nil, errors.Proto(err)
    }

	service := protocol.ServiceId{
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

