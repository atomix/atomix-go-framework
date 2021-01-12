package set

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	set "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/client"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
	"io"
)

const PrimitiveType client.PrimitiveType = "Set"

func NewClient(id client.ID, name string, conn *grpc.ClientConn) Client {
	return &setClient{
		PrimitiveClient: client.NewPrimitiveClient(id, PrimitiveType, name, conn),
		client:          set.NewSetServiceClient(conn),
		log:             logging.GetLogger("atomix", "client", "set"),
	}
}

type Client interface {
	client.PrimitiveClient
	// Size gets the number of elements in the set
	Size(context.Context) (*set.SizeOutput, error)
	// Contains returns whether the set contains a value
	Contains(context.Context, *set.ContainsInput) (*set.ContainsOutput, error)
	// Add adds a value to the set
	Add(context.Context, *set.AddInput) (*set.AddOutput, error)
	// Remove removes a value from the set
	Remove(context.Context, *set.RemoveInput) (*set.RemoveOutput, error)
	// Clear removes all values from the set
	Clear(context.Context) error
	// Events listens for set change events
	Events(context.Context, *set.EventsInput, chan<- set.EventsOutput) error
	// Elements lists all elements in the set
	Elements(context.Context, *set.ElementsInput, chan<- set.ElementsOutput) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context, chan<- set.SnapshotEntry) error
	// Restore imports a snapshot of the primitive state
}

type setClient struct {
	client.PrimitiveClient
	client set.SetServiceClient
	log    logging.Logger
}

func (c *setClient) getRequestHeader() primitiveapi.RequestHeader {
	return primitiveapi.RequestHeader{
		PrimitiveID: primitiveapi.PrimitiveId{
			Type: string(c.Type()),
			Name: c.Name(),
		},
	}
}

func (c *setClient) Size(ctx context.Context) (*set.SizeOutput, error) {
	request := &set.SizeRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.Size(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *setClient) Contains(ctx context.Context, input *set.ContainsInput) (*set.ContainsOutput, error) {
	request := &set.ContainsRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Contains(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *setClient) Add(ctx context.Context, input *set.AddInput) (*set.AddOutput, error) {
	request := &set.AddRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Add(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *setClient) Remove(ctx context.Context, input *set.RemoveInput) (*set.RemoveOutput, error) {
	request := &set.RemoveRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Remove(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *setClient) Clear(ctx context.Context) error {
	request := &set.ClearRequest{
		Header: c.getRequestHeader(),
	}
	_, err := c.client.Clear(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (c *setClient) Events(ctx context.Context, input *set.EventsInput, ch chan<- set.EventsOutput) error {
	request := &set.EventsRequest{
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
	}()

	select {
	case <-handshakeCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *setClient) Elements(ctx context.Context, input *set.ElementsInput, ch chan<- set.ElementsOutput) error {
	request := &set.ElementsRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input

	stream, err := c.client.Elements(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	handshakeCh := make(chan struct{})
	go func() {
		defer close(ch)
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
	}()

	select {
	case <-handshakeCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *setClient) Snapshot(ctx context.Context, ch chan<- set.SnapshotEntry) error {
	request := &set.SnapshotRequest{
		Header: c.getRequestHeader(),
	}

	stream, err := c.client.Snapshot(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	handshakeCh := make(chan struct{})
	go func() {
		defer close(ch)
		response, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			c.log.Error(err)
		} else {
			switch response.Header.ResponseType {
			case primitiveapi.ResponseType_RESPONSE:
				ch <- response.Entry
			case primitiveapi.ResponseType_RESPONSE_STREAM:
				close(handshakeCh)
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

var _ Client = &setClient{}
