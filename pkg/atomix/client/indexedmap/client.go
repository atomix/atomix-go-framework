package indexedmap

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	indexedmap "github.com/atomix/api/go/atomix/primitive/indexedmap"
	"github.com/atomix/go-framework/pkg/atomix/client"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
	"io"
)

const PrimitiveType client.PrimitiveType = "IndexedMap"

func NewClient(id client.ID, name string, conn *grpc.ClientConn) Client {
	return &indexedMapClient{
		PrimitiveClient: client.NewPrimitiveClient(id, PrimitiveType, name, conn),
		client:          indexedmap.NewIndexedMapServiceClient(conn),
		log:             logging.GetLogger("atomix", "client", "indexedmap"),
	}
}

type Client interface {
	client.PrimitiveClient
	// Size returns the size of the map
	Size(context.Context) (*indexedmap.SizeOutput, error)
	// Exists checks whether a key exists in the map
	Exists(context.Context, *indexedmap.ExistsInput) (*indexedmap.ExistsOutput, error)
	// Put puts an entry into the map
	Put(context.Context, *indexedmap.PutInput) (*indexedmap.PutOutput, error)
	// Get gets the entry for a key
	Get(context.Context, *indexedmap.GetInput) (*indexedmap.GetOutput, error)
	// FirstEntry gets the first entry in the map
	FirstEntry(context.Context) (*indexedmap.FirstEntryOutput, error)
	// LastEntry gets the last entry in the map
	LastEntry(context.Context) (*indexedmap.LastEntryOutput, error)
	// PrevEntry gets the previous entry in the map
	PrevEntry(context.Context, *indexedmap.PrevEntryInput) (*indexedmap.PrevEntryOutput, error)
	// NextEntry gets the next entry in the map
	NextEntry(context.Context, *indexedmap.NextEntryInput) (*indexedmap.NextEntryOutput, error)
	// Remove removes an entry from the map
	Remove(context.Context, *indexedmap.RemoveInput) (*indexedmap.RemoveOutput, error)
	// Clear removes all entries from the map
	Clear(context.Context) error
	// Events listens for change events
	Events(context.Context, *indexedmap.EventsInput, chan<- indexedmap.EventsOutput) error
	// Entries lists all entries in the map
	Entries(context.Context, *indexedmap.EntriesInput, chan<- indexedmap.EntriesOutput) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context, chan<- indexedmap.SnapshotEntry) error
	// Restore imports a snapshot of the primitive state
}

type indexedMapClient struct {
	client.PrimitiveClient
	client indexedmap.IndexedMapServiceClient
	log    logging.Logger
}

func (c *indexedMapClient) getRequestHeader() primitiveapi.RequestHeader {
	return primitiveapi.RequestHeader{
		PrimitiveID: primitiveapi.PrimitiveId{
			Type: string(c.Type()),
			Name: c.Name(),
		},
	}
}

func (c *indexedMapClient) Size(ctx context.Context) (*indexedmap.SizeOutput, error) {
	request := &indexedmap.SizeRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.Size(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *indexedMapClient) Exists(ctx context.Context, input *indexedmap.ExistsInput) (*indexedmap.ExistsOutput, error) {
	request := &indexedmap.ExistsRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Exists(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *indexedMapClient) Put(ctx context.Context, input *indexedmap.PutInput) (*indexedmap.PutOutput, error) {
	request := &indexedmap.PutRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Put(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *indexedMapClient) Get(ctx context.Context, input *indexedmap.GetInput) (*indexedmap.GetOutput, error) {
	request := &indexedmap.GetRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Get(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *indexedMapClient) FirstEntry(ctx context.Context) (*indexedmap.FirstEntryOutput, error) {
	request := &indexedmap.FirstEntryRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.FirstEntry(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *indexedMapClient) LastEntry(ctx context.Context) (*indexedmap.LastEntryOutput, error) {
	request := &indexedmap.LastEntryRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.LastEntry(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *indexedMapClient) PrevEntry(ctx context.Context, input *indexedmap.PrevEntryInput) (*indexedmap.PrevEntryOutput, error) {
	request := &indexedmap.PrevEntryRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.PrevEntry(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *indexedMapClient) NextEntry(ctx context.Context, input *indexedmap.NextEntryInput) (*indexedmap.NextEntryOutput, error) {
	request := &indexedmap.NextEntryRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.NextEntry(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *indexedMapClient) Remove(ctx context.Context, input *indexedmap.RemoveInput) (*indexedmap.RemoveOutput, error) {
	request := &indexedmap.RemoveRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Remove(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *indexedMapClient) Clear(ctx context.Context) error {
	request := &indexedmap.ClearRequest{
		Header: c.getRequestHeader(),
	}
	_, err := c.client.Clear(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (c *indexedMapClient) Events(ctx context.Context, input *indexedmap.EventsInput, ch chan<- indexedmap.EventsOutput) error {
	request := &indexedmap.EventsRequest{
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

func (c *indexedMapClient) Entries(ctx context.Context, input *indexedmap.EntriesInput, ch chan<- indexedmap.EntriesOutput) error {
	request := &indexedmap.EntriesRequest{
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

func (c *indexedMapClient) Snapshot(ctx context.Context, ch chan<- indexedmap.SnapshotEntry) error {
	request := &indexedmap.SnapshotRequest{
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

var _ Client = &indexedMapClient{}
