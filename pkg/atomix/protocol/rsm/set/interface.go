package set

import (
	set "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/golang/protobuf/proto"
	"io"
)

type ServiceEventsStream interface {
	// ID returns the stream identifier
	ID() rsm.StreamID

	// OperationID returns the stream operation identifier
	OperationID() rsm.OperationID

	// Session returns the stream session
	Session() rsm.Session

	// Notify sends a value on the stream
	Notify(value *set.EventsOutput) error

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

func (s *ServiceAdaptorEventsStream) Notify(value *set.EventsOutput) error {
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

type ServiceElementsStream interface {
	// ID returns the stream identifier
	ID() rsm.StreamID

	// OperationID returns the stream operation identifier
	OperationID() rsm.OperationID

	// Session returns the stream session
	Session() rsm.Session

	// Notify sends a value on the stream
	Notify(value *set.ElementsOutput) error

	// Close closes the stream
	Close()
}

func newServiceElementsStream(stream rsm.Stream) ServiceElementsStream {
	return &ServiceAdaptorElementsStream{
		stream: stream,
	}
}

type ServiceAdaptorElementsStream struct {
	stream rsm.Stream
}

func (s *ServiceAdaptorElementsStream) ID() rsm.StreamID {
	return s.stream.ID()
}

func (s *ServiceAdaptorElementsStream) OperationID() rsm.OperationID {
	return s.stream.OperationID()
}

func (s *ServiceAdaptorElementsStream) Session() rsm.Session {
	return s.stream.Session()
}

func (s *ServiceAdaptorElementsStream) Notify(value *set.ElementsOutput) error {
	bytes, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	s.stream.Value(bytes)
	return nil
}

func (s *ServiceAdaptorElementsStream) Close() {
	s.stream.Close()
}

var _ ServiceElementsStream = &ServiceAdaptorElementsStream{}

type ServiceSnapshotWriter interface {
	// Write writes a value to the stream
	Write(value *set.SnapshotEntry) error

	// Close closes the stream
	Close()
}

func newServiceSnapshotWriter(writer io.Writer) ServiceSnapshotWriter {
	return &ServiceAdaptorSnapshotWriter{
		writer: writer,
	}
}

type ServiceAdaptorSnapshotWriter struct {
	writer io.Writer
}

func (s *ServiceAdaptorSnapshotWriter) Write(value *set.SnapshotEntry) error {
	bytes, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	return util.WriteBytes(s.writer, bytes)
}

func (s *ServiceAdaptorSnapshotWriter) Close() {

}

var _ ServiceSnapshotWriter = &ServiceAdaptorSnapshotWriter{}

func newServiceSnapshotStreamWriter(stream rsm.Stream) ServiceSnapshotWriter {
	return &ServiceAdaptorSnapshotStreamWriter{
		stream: stream,
	}
}

type ServiceAdaptorSnapshotStreamWriter struct {
	stream rsm.Stream
}

func (s *ServiceAdaptorSnapshotStreamWriter) Write(value *set.SnapshotEntry) error {
	bytes, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	s.stream.Value(bytes)
	return nil
}

func (s *ServiceAdaptorSnapshotStreamWriter) Close() {
	s.stream.Close()
}

var _ ServiceSnapshotWriter = &ServiceAdaptorSnapshotStreamWriter{}

type Service interface {
	// Size gets the number of elements in the set
	Size() (*set.SizeOutput, error)
	// Contains returns whether the set contains a value
	Contains(*set.ContainsInput) (*set.ContainsOutput, error)
	// Add adds a value to the set
	Add(*set.AddInput) (*set.AddOutput, error)
	// Remove removes a value from the set
	Remove(*set.RemoveInput) (*set.RemoveOutput, error)
	// Clear removes all values from the set
	Clear() error
	// Events listens for set change events
	Events(*set.EventsInput, ServiceEventsStream) error
	// Elements lists all elements in the set
	Elements(*set.ElementsInput, ServiceElementsStream) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot(ServiceSnapshotWriter) error
	// Restore imports a snapshot of the primitive state
	Restore(*set.SnapshotEntry) error
}
