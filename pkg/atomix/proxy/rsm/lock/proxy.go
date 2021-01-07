package lock

import (
	"context"
	lock "github.com/atomix/api/go/atomix/primitive/lock"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

const Type = "Lock"

const (
	lockOp     = "Lock"
	unlockOp   = "Unlock"
	isLockedOp = "IsLocked"
	snapshotOp = "Snapshot"
	restoreOp  = "Restore"
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

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request LockRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, lockOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request LockRequest failed: %v", err)
		return nil, err
	}

	response := &lock.LockResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request LockRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending LockResponse %+v", response)
	return response, nil
}

func (s *Proxy) Unlock(ctx context.Context, request *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	s.log.Debugf("Received UnlockRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request UnlockRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, unlockOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request UnlockRequest failed: %v", err)
		return nil, err
	}

	response := &lock.UnlockResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request UnlockRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending UnlockResponse %+v", response)
	return response, nil
}

func (s *Proxy) IsLocked(ctx context.Context, request *lock.IsLockedRequest) (*lock.IsLockedResponse, error) {
	s.log.Debugf("Received IsLockedRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request IsLockedRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoQuery(ctx, isLockedOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request IsLockedRequest failed: %v", err)
		return nil, err
	}

	response := &lock.IsLockedResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request IsLockedRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending IsLockedResponse %+v", response)
	return response, nil
}

func (s *Proxy) Snapshot(ctx context.Context, request *lock.SnapshotRequest) (*lock.SnapshotResponse, error) {
	s.log.Debugf("Received SnapshotRequest %+v", request)

	var err error
	var inputBytes []byte
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, snapshotOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return nil, err
	}

	response := &lock.SnapshotResponse{}
	output := &response.Snapshot
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending SnapshotResponse %+v", response)
	return response, nil
}

func (s *Proxy) Restore(ctx context.Context, request *lock.RestoreRequest) (*lock.RestoreResponse, error) {
	s.log.Debugf("Received RestoreRequest %+v", request)

	var err error
	input := &request.Snapshot
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request RestoreRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	_, err = partition.DoCommand(ctx, restoreOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request RestoreRequest failed: %v", err)
		return nil, err
	}

	response := &lock.RestoreResponse{}
	s.log.Debugf("Sending RestoreResponse %+v", response)
	return response, nil
}
