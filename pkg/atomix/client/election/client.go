package election

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	election "github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/go-framework/pkg/atomix/client"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
	"io"
)

const PrimitiveType client.PrimitiveType = "Election"

func NewClient(id client.ID, name string, conn *grpc.ClientConn) Client {
	return &electionClient{
		PrimitiveClient: client.NewPrimitiveClient(id, PrimitiveType, name, conn),
		client:          election.NewLeaderElectionServiceClient(conn),
		log:             logging.GetLogger("atomix", "client", "election"),
	}
}

type Client interface {
	client.PrimitiveClient
	// Enter enters the leader election
	Enter(context.Context, *election.EnterInput) (*election.EnterOutput, error)
	// Withdraw withdraws a candidate from the leader election
	Withdraw(context.Context, *election.WithdrawInput) (*election.WithdrawOutput, error)
	// Anoint anoints a candidate leader
	Anoint(context.Context, *election.AnointInput) (*election.AnointOutput, error)
	// Promote promotes a candidate
	Promote(context.Context, *election.PromoteInput) (*election.PromoteOutput, error)
	// Evict evicts a candidate from the election
	Evict(context.Context, *election.EvictInput) (*election.EvictOutput, error)
	// GetTerm gets the current leadership term
	GetTerm(context.Context, *election.GetTermInput) (*election.GetTermOutput, error)
	// Events listens for leadership events
	Events(context.Context, *election.EventsInput, chan<- election.EventsOutput) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context) (*election.Snapshot, error)
	// Restore imports a snapshot of the primitive state
	Restore(context.Context, *election.Snapshot) error
}

type electionClient struct {
	client.PrimitiveClient
	client election.LeaderElectionServiceClient
	log    logging.Logger
}

func (c *electionClient) getRequestHeader() primitiveapi.RequestHeader {
	return primitiveapi.RequestHeader{
		PrimitiveID: primitiveapi.PrimitiveId{
			Type: string(c.Type()),
			Name: c.Name(),
		},
	}
}

func (c *electionClient) Enter(ctx context.Context, input *election.EnterInput) (*election.EnterOutput, error) {
	request := &election.EnterRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Enter(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *electionClient) Withdraw(ctx context.Context, input *election.WithdrawInput) (*election.WithdrawOutput, error) {
	request := &election.WithdrawRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Withdraw(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *electionClient) Anoint(ctx context.Context, input *election.AnointInput) (*election.AnointOutput, error) {
	request := &election.AnointRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Anoint(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *electionClient) Promote(ctx context.Context, input *election.PromoteInput) (*election.PromoteOutput, error) {
	request := &election.PromoteRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Promote(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *electionClient) Evict(ctx context.Context, input *election.EvictInput) (*election.EvictOutput, error) {
	request := &election.EvictRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.Evict(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *electionClient) GetTerm(ctx context.Context, input *election.GetTermInput) (*election.GetTermOutput, error) {
	request := &election.GetTermRequest{
		Header: c.getRequestHeader(),
	}
	request.Input = *input
	response, err := c.client.GetTerm(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Output, nil
}

func (c *electionClient) Events(ctx context.Context, input *election.EventsInput, ch chan<- election.EventsOutput) error {
	request := &election.EventsRequest{
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

func (c *electionClient) Snapshot(ctx context.Context) (*election.Snapshot, error) {
	request := &election.SnapshotRequest{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.Snapshot(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &response.Snapshot, nil
}

func (c *electionClient) Restore(ctx context.Context, input *election.Snapshot) error {
	request := &election.RestoreRequest{
		Header: c.getRequestHeader(),
	}
	request.Snapshot = *input
	_, err := c.client.Restore(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

var _ Client = &electionClient{}
