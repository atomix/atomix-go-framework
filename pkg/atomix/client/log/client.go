package log

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	log "github.com/atomix/api/go/atomix/primitive/log"
	"github.com/atomix/go-framework/pkg/atomix/client"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"google.golang.org/grpc"
	"io"
)

const PrimitiveType client.PrimitiveType = "Log"

func NewClient(name string, conn *grpc.ClientConn) Client {
	return &logClient{
		PrimitiveClient: client.NewPrimitiveClient(PrimitiveType, name, conn),
		client:          log.NewLogServiceClient(conn),
		log:             logging.GetLogger("atomix", "client", "log"),
	}
}

type Client interface {
	client.PrimitiveClient
	// Size returns the size of the log
	Size(context.Context) (*log.SizeOutput, error)
	// Exists checks whether an index exists in the log
	Exists(context.Context, *log.ExistsInput) (*log.ExistsOutput, error)
	// Appends appends an entry into the log
	Append(context.Context, *log.AppendInput) (*log.AppendOutput, error)
	// Get gets the entry for an index
	Get(context.Context, *log.GetInput) (*log.GetOutput, error)
	// FirstEntry gets the first entry in the log
	FirstEntry(context.Context) (*log.FirstEntryOutput, error)
	// LastEntry gets the last entry in the log
	LastEntry(context.Context) (*log.LastEntryOutput, error)
	// PrevEntry gets the previous entry in the log
	PrevEntry(context.Context, *log.PrevEntryInput) (*log.PrevEntryOutput, error)
	// NextEntry gets the next entry in the log
	NextEntry(context.Context, *log.NextEntryInput) (*log.NextEntryOutput, error)
	// Remove removes an entry from the log
	Remove(context.Context, *log.RemoveInput) (*log.RemoveOutput, error)
	// Clear removes all entries from the log
	Clear(context.Context) error
	// Events listens for change events
	Events(context.Context, *log.EventsInput, chan<- log.EventsOutput) error
	// Entries lists all entries in the log
	Entries(context.Context, *log.EntriesInput, chan<- log.EntriesOutput) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context, chan<- log.SnapshotEntry) error
	// Restore imports a snapshot of the primitive state
}

type logClient struct {
	client.PrimitiveClient
	client log.LogServiceClient
	log    logging.Logger
}

func (c *logClient) getRequestHeader() primitiveapi.RequestHeader {
	return primitiveapi.RequestHeader{
		PrimitiveID: primitiveapi.PrimitiveId{
			Type: string(c.Type()),
			Name: c.Name(),
		},
	}
}

func (c *logClient) Size(ctx context.Context) (*log.SizeOutput, error) {
	request := &log.SizeRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.Size(ctx, request)
	if err != nil {
		return nil, err
	}
	return &response.Output, nil
}

func (c *logClient) Exists(ctx context.Context, input *log.ExistsInput) (*log.ExistsOutput, error) {
	request := &log.ExistsRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Exists(ctx, request)
	if err != nil {
		return nil, err
	}
	return &response.Output, nil
}

func (c *logClient) Append(ctx context.Context, input *log.AppendInput) (*log.AppendOutput, error) {
	request := &log.AppendRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Append(ctx, request)
	if err != nil {
		return nil, err
	}
	return &response.Output, nil
}

func (c *logClient) Get(ctx context.Context, input *log.GetInput) (*log.GetOutput, error) {
	request := &log.GetRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Get(ctx, request)
	if err != nil {
		return nil, err
	}
	return &response.Output, nil
}

func (c *logClient) FirstEntry(ctx context.Context) (*log.FirstEntryOutput, error) {
	request := &log.FirstEntryRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.FirstEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	return &response.Output, nil
}

func (c *logClient) LastEntry(ctx context.Context) (*log.LastEntryOutput, error) {
	request := &log.LastEntryRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.LastEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	return &response.Output, nil
}

func (c *logClient) PrevEntry(ctx context.Context, input *log.PrevEntryInput) (*log.PrevEntryOutput, error) {
	request := &log.PrevEntryRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.PrevEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	return &response.Output, nil
}

func (c *logClient) NextEntry(ctx context.Context, input *log.NextEntryInput) (*log.NextEntryOutput, error) {
	request := &log.NextEntryRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.NextEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	return &response.Output, nil
}

func (c *logClient) Remove(ctx context.Context, input *log.RemoveInput) (*log.RemoveOutput, error) {
	request := &log.RemoveRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}
	return &response.Output, nil
}

func (c *logClient) Clear(ctx context.Context) error {
	request := &log.ClearRequest{
		Header: c.getRequestHeader(),
	}
	_, err := c.client.Clear(ctx, request)
	return err
}

func (c *logClient) Events(ctx context.Context, input *log.EventsInput, ch chan<- log.EventsOutput) error {
	request := &log.EventsRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input

	stream, err := c.client.Events(ctx, request)
	if err != nil {
		return err
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

func (c *logClient) Entries(ctx context.Context, input *log.EntriesInput, ch chan<- log.EntriesOutput) error {
	request := &log.EntriesRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input

	stream, err := c.client.Entries(ctx, request)
	if err != nil {
		return err
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

func (c *logClient) Snapshot(ctx context.Context, ch chan<- log.SnapshotEntry) error {
	request := &log.SnapshotRequest{
		Header: c.getRequestHeader(),
	}

	stream, err := c.client.Snapshot(ctx, request)
	if err != nil {
		return err
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

var _ Client = &logClient{}
