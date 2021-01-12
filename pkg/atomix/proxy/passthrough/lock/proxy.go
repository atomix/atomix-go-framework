package lock

import (
	"context"
	lock "github.com/atomix/api/go/atomix/primitive/lock"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy/passthrough"
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
func RegisterProxy(node *passthrough.Node) {
	node.RegisterProxy("Lock", func(server *grpc.Server, client *passthrough.Client) {
		lock.RegisterLockServiceServer(server, &Proxy{
			Proxy: passthrough.NewProxy(client),
			log:   logging.GetLogger("atomix", "lock"),
		})
	})
}

type Proxy struct {
	*passthrough.Proxy
	log logging.Logger
}

func (s *Proxy) Lock(ctx context.Context, request *lock.LockRequest) (*lock.LockResponse, error) {
	s.log.Debugf("Received LockRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := lock.NewLockServiceClient(conn)
	response, err := client.Lock(ctx, request)
	if err != nil {
		s.log.Errorf("Request LockRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending LockResponse %+v", response)
	return response, nil
}

func (s *Proxy) Unlock(ctx context.Context, request *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	s.log.Debugf("Received UnlockRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := lock.NewLockServiceClient(conn)
	response, err := client.Unlock(ctx, request)
	if err != nil {
		s.log.Errorf("Request UnlockRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending UnlockResponse %+v", response)
	return response, nil
}

func (s *Proxy) IsLocked(ctx context.Context, request *lock.IsLockedRequest) (*lock.IsLockedResponse, error) {
	s.log.Debugf("Received IsLockedRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := lock.NewLockServiceClient(conn)
	response, err := client.IsLocked(ctx, request)
	if err != nil {
		s.log.Errorf("Request IsLockedRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending IsLockedResponse %+v", response)
	return response, nil
}

func (s *Proxy) Snapshot(ctx context.Context, request *lock.SnapshotRequest) (*lock.SnapshotResponse, error) {
	s.log.Debugf("Received SnapshotRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := lock.NewLockServiceClient(conn)
	response, err := client.Snapshot(ctx, request)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending SnapshotResponse %+v", response)
	return response, nil
}

func (s *Proxy) Restore(ctx context.Context, request *lock.RestoreRequest) (*lock.RestoreResponse, error) {
	s.log.Debugf("Received RestoreRequest %+v", request)
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := lock.NewLockServiceClient(conn)
	response, err := client.Restore(ctx, request)
	if err != nil {
		s.log.Errorf("Request RestoreRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending RestoreResponse %+v", response)
	return response, nil
}
