package list

import (
	list "github.com/atomix/api/go/atomix/primitive/list"
	"github.com/atomix/go-framework/pkg/atomix/storage/rsm"
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
	Notify(value *list.EventsOutput) error

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

func (s *ServiceAdaptorEventsStream) Notify(value *list.EventsOutput) error {
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
	Notify(value *list.ElementsOutput) error

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

func (s *ServiceAdaptorElementsStream) Notify(value *list.ElementsOutput) error {
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
	Write(value *list.SnapshotEntry) error

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

func (s *ServiceAdaptorSnapshotWriter) Write(value *list.SnapshotEntry) error {
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

func (s *ServiceAdaptorSnapshotStreamWriter) Write(value *list.SnapshotEntry) error {
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
	// Size gets the number of elements in the list
	Size() (*list.SizeOutput, error)
	// Contains returns whether the list contains a value
	Contains(*list.ContainsInput) (*list.ContainsOutput, error)
	// Append appends a value to the list
	Append(*list.AppendInput) (*list.AppendOutput, error)
	// Insert inserts a value at a specific index in the list
	Insert(*list.InsertInput) (*list.InsertOutput, error)
	// Get gets the value at an index in the list
	Get(*list.GetInput) (*list.GetOutput, error)
	// Set sets the value at an index in the list
	Set(*list.SetInput) (*list.SetOutput, error)
	// Remove removes an element from the list
	Remove(*list.RemoveInput) (*list.RemoveOutput, error)
	// Clear removes all elements from the list
	Clear() error
	// Events listens for change events
	Events(*list.EventsInput, ServiceEventsStream) error
	// Elements streams all elements in the list
	Elements(*list.ElementsInput, ServiceElementsStream) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot(ServiceSnapshotWriter) error
	// Restore imports a snapshot of the primitive state
	Restore(*list.SnapshotEntry) error
}
