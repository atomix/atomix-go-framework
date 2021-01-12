package _map

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/client"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
	"io"
)

const PrimitiveType client.PrimitiveType = "Map"

func NewClient(id client.ID, name string, conn *grpc.ClientConn) Client {
	return &mapClient{
		PrimitiveClient: client.NewPrimitiveClient(id, PrimitiveType, name, conn),
		client:          _map.NewMapServiceClient(conn),
		log:             logging.GetLogger("atomix", "client", "map"),
	}
}

type Client interface {
	client.PrimitiveClient
	// Size returns the size of the map
	Size(context.Context) (*_map.SizeOutput, error)
	// Exists checks whether a key exists in the map
	Exists(context.Context, *_map.ExistsInput) (*_map.ExistsOutput, error)
	// Put puts an entry into the map
	Put(context.Context, *_map.PutInput) (*_map.PutOutput, error)
	// Get gets the entry for a key
	Get(context.Context, *_map.GetInput) (*_map.GetOutput, error)
	// Remove removes an entry from the map
	Remove(context.Context, *_map.RemoveInput) (*_map.RemoveOutput, error)
	// Clear removes all entries from the map
	Clear(context.Context) error
	// Events listens for change events
	Events(context.Context, *_map.EventsInput, chan<- _map.EventsOutput) error
	// Entries lists all entries in the map
	Entries(context.Context, *_map.EntriesInput, chan<- _map.EntriesOutput) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context, chan<- _map.SnapshotEntry) error
	// Restore imports a snapshot of the primitive state
}

type mapClient struct {
	client.PrimitiveClient
	client _map.MapServiceClient
	log    logging.Logger
}

func (c *mapClient) getRequestHeader() primitiveapi.RequestHeader {
	return primitiveapi.RequestHeader{
		PrimitiveID: primitiveapi.PrimitiveId{
			Type: string(c.Type()),
			Name: c.Name(),
		},
	}
}

func (c *mapClient) Size(ctx context.Context) (*_map.SizeOutput, error) {
	request := &_map.SizeRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.Size(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *mapClient) Exists(ctx context.Context, input *_map.ExistsInput) (*_map.ExistsOutput, error) {
	request := &_map.ExistsRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Exists(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *mapClient) Put(ctx context.Context, input *_map.PutInput) (*_map.PutOutput, error) {
	request := &_map.PutRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Put(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *mapClient) Get(ctx context.Context, input *_map.GetInput) (*_map.GetOutput, error) {
	request := &_map.GetRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Get(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *mapClient) Remove(ctx context.Context, input *_map.RemoveInput) (*_map.RemoveOutput, error) {
	request := &_map.RemoveRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Remove(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *mapClient) Clear(ctx context.Context) error {
	request := &_map.ClearRequest{
		Header: c.getRequestHeader(),
	}
	_, err := c.client.Clear(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (c *mapClient) Events(ctx context.Context, input *_map.EventsInput, ch chan<- _map.EventsOutput) error {
	request := &_map.EventsRequest{
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

func (c *mapClient) Entries(ctx context.Context, input *_map.EntriesInput, ch chan<- _map.EntriesOutput) error {
	request := &_map.EntriesRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input

	stream, err := c.client.Entries(ctx, request)
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

func (c *mapClient) Snapshot(ctx context.Context, ch chan<- _map.SnapshotEntry) error {
	request := &_map.SnapshotRequest{
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

var _ Client = &mapClient{}
