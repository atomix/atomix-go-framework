package leader

import (
	leader "github.com/atomix/api/go/atomix/primitive/leader"
	"github.com/atomix/go-framework/pkg/atomix/storage/rsm"
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
	Notify(value *leader.EventsOutput) error

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

func (s *ServiceAdaptorEventsStream) Notify(value *leader.EventsOutput) error {
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
	// Latch attempts to acquire the leader latch
	Latch(*leader.LatchInput) (*leader.LatchOutput, error)
	// Get gets the current leader
	Get(*leader.GetInput) (*leader.GetOutput, error)
	// Events listens for leader change events
	Events(*leader.EventsInput, ServiceEventsStream) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot() (*leader.Snapshot, error)
	// Restore imports a snapshot of the primitive state
	Restore(*leader.Snapshot) error
}
