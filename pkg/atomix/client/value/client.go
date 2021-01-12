package value

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/client"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
	"io"
)

const PrimitiveType client.PrimitiveType = "Value"

func NewClient(id client.ID, name string, conn *grpc.ClientConn) Client {
	return &valueClient{
		PrimitiveClient: client.NewPrimitiveClient(id, PrimitiveType, name, conn),
		client:          value.NewValueServiceClient(conn),
		log:             logging.GetLogger("atomix", "client", "value"),
	}
}

type Client interface {
	client.PrimitiveClient
	// Set sets the value
	Set(context.Context, *value.SetInput) (*value.SetOutput, error)
	// Get gets the value
	Get(context.Context, *value.GetInput) (*value.GetOutput, error)
	// Events listens for value change events
	Events(context.Context, *value.EventsInput, chan<- value.EventsOutput) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context) (*value.Snapshot, error)
	// Restore imports a snapshot of the primitive state
	Restore(context.Context, *value.Snapshot) error
}

type valueClient struct {
	client.PrimitiveClient
	client value.ValueServiceClient
	log    logging.Logger
}

func (c *valueClient) getRequestHeader() primitiveapi.RequestHeader {
	return primitiveapi.RequestHeader{
		PrimitiveID: primitiveapi.PrimitiveId{
			Type: string(c.Type()),
			Name: c.Name(),
		},
	}
}

func (c *valueClient) Set(ctx context.Context, input *value.SetInput) (*value.SetOutput, error) {
	request := &value.SetRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Set(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *valueClient) Get(ctx context.Context, input *value.GetInput) (*value.GetOutput, error) {
	request := &value.GetRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Get(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *valueClient) Events(ctx context.Context, input *value.EventsInput, ch chan<- value.EventsOutput) error {
	request := &value.EventsRequest{
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

func (c *valueClient) Snapshot(ctx context.Context) (*value.Snapshot, error) {
	request := &value.SnapshotRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.Snapshot(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Snapshot, nil
}

func (c *valueClient) Restore(ctx context.Context, input *value.Snapshot) error {
	request := &value.RestoreRequest{
		Header: c.getRequestHeader(),
	}
	request.Snapshot = *input
	_, err := c.client.Restore(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

var _ Client = &valueClient{}
