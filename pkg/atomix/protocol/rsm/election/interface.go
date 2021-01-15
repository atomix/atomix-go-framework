package election

import (
	election "github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	"github.com/golang/protobuf/proto"
)

type ServiceEventsStream interface {
	// ID returns the stream identifier
	ID() rsm.StreamID

	// OperationID returns the stream operation identifier
	OperationID() rsm.OperationID

	// Session returns the stream session
	Session() rsm.Session

	// Notify sends a value on the stream
	Notify(value *election.EventsOutput) error

	// Close closes the stream
	Close()
}

func newServiceEventsStream(stream rsm.Stream) ServiceEventsStream {
	return &ServiceAdaptorEventsStream{
		stream: stream,
	}
}

type ServiceAdaptorEventsStream struct {
	stream rsm.Stream
}

func (s *ServiceAdaptorEventsStream) ID() rsm.StreamID {
	return s.stream.ID()
}

func (s *ServiceAdaptorEventsStream) OperationID() rsm.OperationID {
	return s.stream.OperationID()
}

func (s *ServiceAdaptorEventsStream) Session() rsm.Session {
	return s.stream.Session()
}

func (s *ServiceAdaptorEventsStream) Notify(value *election.EventsOutput) error {
	bytes, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	s.stream.Value(bytes)
	return nil
}

func (s *ServiceAdaptorEventsStream) Close() {
	s.stream.Close()
}

var _ ServiceEventsStream = &ServiceAdaptorEventsStream{}

type Service interface {
	// Enter enters the leader election
	Enter(*election.EnterInput) (*election.EnterOutput, error)
	// Withdraw withdraws a candidate from the leader election
	Withdraw(*election.WithdrawInput) (*election.WithdrawOutput, error)
	// Anoint anoints a candidate leader
	Anoint(*election.AnointInput) (*election.AnointOutput, error)
	// Promote promotes a candidate
	Promote(*election.PromoteInput) (*election.PromoteOutput, error)
	// Evict evicts a candidate from the election
	Evict(*election.EvictInput) (*election.EvictOutput, error)
	// GetTerm gets the current leadership term
	GetTerm(*election.GetTermInput) (*election.GetTermOutput, error)
	// Events listens for leadership events
	Events(*election.EventsInput, ServiceEventsStream) (rsm.StreamCloser, error)
	// Snapshot exports a snapshot of the primitive state
	Snapshot() (*election.Snapshot, error)
	// Restore imports a snapshot of the primitive state
	Restore(*election.Snapshot) error
}
