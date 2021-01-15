package counter

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/client"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
)

const PrimitiveType client.PrimitiveType = "Counter"

func NewClient(id client.ID, name string, conn *grpc.ClientConn) Client {
	return &counterClient{
		PrimitiveClient: client.NewPrimitiveClient(id, PrimitiveType, name, conn),
		client:          counter.NewCounterServiceClient(conn),
		log:             logging.GetLogger("atomix", "client", "counter"),
	}
}

type Client interface {
	client.PrimitiveClient
	// Set sets the counter value
	Set(context.Context, *counter.SetInput) (*counter.SetOutput, error)
	// Get gets the current counter value
	Get(context.Context, *counter.GetInput) (*counter.GetOutput, error)
	// Increment increments the counter value
	Increment(context.Context, *counter.IncrementInput) (*counter.IncrementOutput, error)
	// Decrement decrements the counter value
	Decrement(context.Context, *counter.DecrementInput) (*counter.DecrementOutput, error)
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context) (*counter.Snapshot, error)
	// Restore imports a snapshot of the primitive state
	Restore(context.Context, *counter.Snapshot) error
}

type counterClient struct {
	client.PrimitiveClient
	client counter.CounterServiceClient
	log    logging.Logger
}

func (c *counterClient) getRequestHeader() primitiveapi.RequestHeader {
	return primitiveapi.RequestHeader{
		PrimitiveID: primitiveapi.PrimitiveId{
			Type: string(c.Type()),
			Name: c.Name(),
		},
	}
}

func (c *counterClient) Set(ctx context.Context, input *counter.SetInput) (*counter.SetOutput, error) {
	request := &counter.SetRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Set(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *counterClient) Get(ctx context.Context, input *counter.GetInput) (*counter.GetOutput, error) {
	request := &counter.GetRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Get(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *counterClient) Increment(ctx context.Context, input *counter.IncrementInput) (*counter.IncrementOutput, error) {
	request := &counter.IncrementRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Increment(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *counterClient) Decrement(ctx context.Context, input *counter.DecrementInput) (*counter.DecrementOutput, error) {
	request := &counter.DecrementRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Decrement(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *counterClient) Snapshot(ctx context.Context) (*counter.Snapshot, error) {
	request := &counter.SnapshotRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.Snapshot(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Snapshot, nil
}

func (c *counterClient) Restore(ctx context.Context, input *counter.Snapshot) error {
	request := &counter.RestoreRequest{
		Header: c.getRequestHeader(),
	}
	request.Snapshot = *input
	_, err := c.client.Restore(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

var _ Client = &counterClient{}
