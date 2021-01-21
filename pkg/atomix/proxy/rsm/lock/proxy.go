package lock

import (
	"context"
	lock "github.com/atomix/api/go/atomix/primitive/lock"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

const Type = "Lock"

const (
	lockOp    = "Lock"
	unlockOp  = "Unlock"
	getLockOp = "GetLock"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *rsm.Node) {
	node.RegisterServer(Type, func(server *grpc.Server, client *rsm.Client) {
		lock.RegisterLockServiceServer(server, &Proxy{
			Proxy: rsm.NewProxy(client),
			log:   logging.GetLogger("atomix", "lock"),
		})
	})
}

type Proxy struct {
	*rsm.Proxy
	log logging.Logger
}

func (s *Proxy) Lock(ctx context.Context, request *lock.LockRequest) (*lock.LockResponse, error) {
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

	output, err := partition.DoCommand(ctx, lockOp, input)
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

func (s *Proxy) Unlock(ctx context.Context, request *lock.UnlockRequest) (*lock.UnlockResponse, error) {
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

	output, err := partition.DoCommand(ctx, unlockOp, input)
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

func (s *Proxy) GetLock(ctx context.Context, request *lock.GetLockRequest) (*lock.GetLockResponse, error) {
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

	output, err := partition.DoQuery(ctx, getLockOp, input)
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
