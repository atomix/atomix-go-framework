package lock

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	lock "github.com/atomix/api/go/atomix/primitive/lock"
	"github.com/atomix/go-framework/pkg/atomix/client"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"google.golang.org/grpc"
)

const PrimitiveType client.PrimitiveType = "Lock"

func NewClient(name string, conn *grpc.ClientConn) Client {
	return &lockClient{
		PrimitiveClient: client.NewPrimitiveClient(PrimitiveType, name, conn),
		client:          lock.NewLockServiceClient(conn),
		log:             logging.GetLogger("atomix", "client", "lock"),
	}
}

type Client interface {
	client.PrimitiveClient
	// Lock attempts to acquire the lock
	Lock(context.Context, *lock.LockInput) (*lock.LockOutput, error)
	// Unlock releases the lock
	Unlock(context.Context, *lock.UnlockInput) (*lock.UnlockOutput, error)
	// IsLocked checks whether the lock is held
	IsLocked(context.Context, *lock.IsLockedInput) (*lock.IsLockedOutput, error)
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context) (*lock.Snapshot, error)
	// Restore imports a snapshot of the primitive state
	Restore(context.Context, *lock.Snapshot) error
}

type lockClient struct {
	client.PrimitiveClient
	client lock.LockServiceClient
	log    logging.Logger
}

func (c *lockClient) getRequestHeader() primitiveapi.RequestHeader {
	return primitiveapi.RequestHeader{
		PrimitiveID: primitiveapi.PrimitiveId{
			Type: string(c.Type()),
			Name: c.Name(),
		},
	}
}

func (c *lockClient) Lock(ctx context.Context, input *lock.LockInput) (*lock.LockOutput, error) {
	request := &lock.LockRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Lock(ctx, request)
	if err != nil {
		return nil, err
	}
	return &response.Output, nil
}

func (c *lockClient) Unlock(ctx context.Context, input *lock.UnlockInput) (*lock.UnlockOutput, error) {
	request := &lock.UnlockRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Unlock(ctx, request)
	if err != nil {
		return nil, err
	}
	return &response.Output, nil
}

func (c *lockClient) IsLocked(ctx context.Context, input *lock.IsLockedInput) (*lock.IsLockedOutput, error) {
	request := &lock.IsLockedRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.IsLocked(ctx, request)
	if err != nil {
		return nil, err
	}
	return &response.Output, nil
}

func (c *lockClient) Snapshot(ctx context.Context) (*lock.Snapshot, error) {
	request := &lock.SnapshotRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.Snapshot(ctx, request)
	if err != nil {
		return nil, err
	}
	return &response.Snapshot, nil
}

func (c *lockClient) Restore(ctx context.Context, input *lock.Snapshot) error {
	request := &lock.RestoreRequest{
		Header: c.getRequestHeader(),
	}
	request.Snapshot = *input
	_, err := c.client.Restore(ctx, request)
	return err
}

var _ Client = &lockClient{}
