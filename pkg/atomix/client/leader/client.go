package leader

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	leader "github.com/atomix/api/go/atomix/primitive/leader"
	"github.com/atomix/go-framework/pkg/atomix/client"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
	"io"
)

const PrimitiveType client.PrimitiveType = "LeaderLatch"

func NewClient(id client.ID, name string, conn *grpc.ClientConn) Client {
	return &leaderLatchClient{
		PrimitiveClient: client.NewPrimitiveClient(id, PrimitiveType, name, conn),
		client:          leader.NewLeaderLatchServiceClient(conn),
		log:             logging.GetLogger("atomix", "client", "leaderlatch"),
	}
}

type Client interface {
	client.PrimitiveClient
	// Latch attempts to acquire the leader latch
	Latch(context.Context, *leader.LatchInput) (*leader.LatchOutput, error)
	// Get gets the current leader
	Get(context.Context, *leader.GetInput) (*leader.GetOutput, error)
	// Events listens for leader change events
	Events(context.Context, *leader.EventsInput, chan<- leader.EventsOutput) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context) (*leader.Snapshot, error)
	// Restore imports a snapshot of the primitive state
	Restore(context.Context, *leader.Snapshot) error
}

type leaderLatchClient struct {
	client.PrimitiveClient
	client leader.LeaderLatchServiceClient
	log    logging.Logger
}

func (c *leaderLatchClient) getRequestHeader() primitiveapi.RequestHeader {
	return primitiveapi.RequestHeader{
		PrimitiveID: primitiveapi.PrimitiveId{
			Type: string(c.Type()),
			Name: c.Name(),
		},
	}
}

func (c *leaderLatchClient) Latch(ctx context.Context, input *leader.LatchInput) (*leader.LatchOutput, error) {
	request := &leader.LatchRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Latch(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *leaderLatchClient) Get(ctx context.Context, input *leader.GetInput) (*leader.GetOutput, error) {
	request := &leader.GetRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Get(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *leaderLatchClient) Events(ctx context.Context, input *leader.EventsInput, ch chan<- leader.EventsOutput) error {
	request := &leader.EventsRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input

	stream, err := c.client.Events(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	handshakeCh := make(chan struct{})
	go func() {
		defer close(ch)
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				c.log.Error(err)
			} else {
				switch response.Header.ResponseType {
				case primitiveapi.ResponseType_RESPONSE:
					ch <- response.Output
				case primitiveapi.ResponseType_RESPONSE_STREAM:
					close(handshakeCh)
				}
			}
		}
	}()

	select {
	case <-handshakeCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *leaderLatchClient) Snapshot(ctx context.Context) (*leader.Snapshot, error) {
	request := &leader.SnapshotRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.Snapshot(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Snapshot, nil
}

func (c *leaderLatchClient) Restore(ctx context.Context, input *leader.Snapshot) error {
	request := &leader.RestoreRequest{
		Header: c.getRequestHeader(),
	}
	request.Snapshot = *input
	_, err := c.client.Restore(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

var _ Client = &leaderLatchClient{}
