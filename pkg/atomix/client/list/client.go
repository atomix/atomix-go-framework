package list

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	list "github.com/atomix/api/go/atomix/primitive/list"
	"github.com/atomix/go-framework/pkg/atomix/client"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
	"io"
)

const PrimitiveType client.PrimitiveType = "List"

func NewClient(id client.ID, name string, conn *grpc.ClientConn) Client {
	return &listClient{
		PrimitiveClient: client.NewPrimitiveClient(id, PrimitiveType, name, conn),
		client:          list.NewListServiceClient(conn),
		log:             logging.GetLogger("atomix", "client", "list"),
	}
}

type Client interface {
	client.PrimitiveClient
	// Size gets the number of elements in the list
	Size(context.Context) (*list.SizeOutput, error)
	// Contains returns whether the list contains a value
	Contains(context.Context, *list.ContainsInput) (*list.ContainsOutput, error)
	// Append appends a value to the list
	Append(context.Context, *list.AppendInput) (*list.AppendOutput, error)
	// Insert inserts a value at a specific index in the list
	Insert(context.Context, *list.InsertInput) (*list.InsertOutput, error)
	// Get gets the value at an index in the list
	Get(context.Context, *list.GetInput) (*list.GetOutput, error)
	// Set sets the value at an index in the list
	Set(context.Context, *list.SetInput) (*list.SetOutput, error)
	// Remove removes an element from the list
	Remove(context.Context, *list.RemoveInput) (*list.RemoveOutput, error)
	// Clear removes all elements from the list
	Clear(context.Context) error
	// Events listens for change events
	Events(context.Context, *list.EventsInput, chan<- list.EventsOutput) error
	// Elements streams all elements in the list
	Elements(context.Context, *list.ElementsInput, chan<- list.ElementsOutput) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context, chan<- list.SnapshotEntry) error
	// Restore imports a snapshot of the primitive state
}

type listClient struct {
	client.PrimitiveClient
	client list.ListServiceClient
	log    logging.Logger
}

func (c *listClient) getRequestHeader() primitiveapi.RequestHeader {
	return primitiveapi.RequestHeader{
		PrimitiveID: primitiveapi.PrimitiveId{
			Type: string(c.Type()),
			Name: c.Name(),
		},
	}
}

func (c *listClient) Size(ctx context.Context) (*list.SizeOutput, error) {
	request := &list.SizeRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.Size(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *listClient) Contains(ctx context.Context, input *list.ContainsInput) (*list.ContainsOutput, error) {
	request := &list.ContainsRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Contains(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *listClient) Append(ctx context.Context, input *list.AppendInput) (*list.AppendOutput, error) {
	request := &list.AppendRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Append(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *listClient) Insert(ctx context.Context, input *list.InsertInput) (*list.InsertOutput, error) {
	request := &list.InsertRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Insert(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *listClient) Get(ctx context.Context, input *list.GetInput) (*list.GetOutput, error) {
	request := &list.GetRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Get(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *listClient) Set(ctx context.Context, input *list.SetInput) (*list.SetOutput, error) {
	request := &list.SetRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Set(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *listClient) Remove(ctx context.Context, input *list.RemoveInput) (*list.RemoveOutput, error) {
	request := &list.RemoveRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Remove(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *listClient) Clear(ctx context.Context) error {
	request := &list.ClearRequest{
		Header: c.getRequestHeader(),
	}
	_, err := c.client.Clear(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (c *listClient) Events(ctx context.Context, input *list.EventsInput, ch chan<- list.EventsOutput) error {
	request := &list.EventsRequest{
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

func (c *listClient) Elements(ctx context.Context, input *list.ElementsInput, ch chan<- list.ElementsOutput) error {
	request := &list.ElementsRequest{
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

func (c *listClient) Snapshot(ctx context.Context, ch chan<- list.SnapshotEntry) error {
	request := &list.SnapshotRequest{
		Header: c.getRequestHeader(),
	}

	stream, err := c.client.Snapshot(ctx, request)
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
					ch <- response.Entry
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

var _ Client = &listClient{}
