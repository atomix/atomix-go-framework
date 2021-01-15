package indexedmap

import (
	indexedmap "github.com/atomix/api/go/atomix/primitive/indexedmap"
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
	Notify(value *indexedmap.EventsOutput) error

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

func (s *ServiceAdaptorEventsStream) Notify(value *indexedmap.EventsOutput) error {
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

type ServiceEntriesStream interface {
	// ID returns the stream identifier
	ID() rsm.StreamID

	// OperationID returns the stream operation identifier
	OperationID() rsm.OperationID

	// Session returns the stream session
	Session() rsm.Session

	// Notify sends a value on the stream
	Notify(value *indexedmap.EntriesOutput) error

	// Close closes the stream
	Close()
}

func newServiceEntriesStream(stream rsm.Stream) ServiceEntriesStream {
	return &ServiceAdaptorEntriesStream{
		stream: stream,
	}
}

type ServiceAdaptorEntriesStream struct {
	stream rsm.Stream
}

func (s *ServiceAdaptorEntriesStream) ID() rsm.StreamID {
	return s.stream.ID()
}

func (s *ServiceAdaptorEntriesStream) OperationID() rsm.OperationID {
	return s.stream.OperationID()
}

func (s *ServiceAdaptorEntriesStream) Session() rsm.Session {
	return s.stream.Session()
}

func (s *ServiceAdaptorEntriesStream) Notify(value *indexedmap.EntriesOutput) error {
	bytes, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	s.stream.Value(bytes)
	return nil
}

func (s *ServiceAdaptorEntriesStream) Close() {
	s.stream.Close()
}

var _ ServiceEntriesStream = &ServiceAdaptorEntriesStream{}

type ServiceSnapshotWriter interface {
	// Write writes a value to the stream
	Write(value *indexedmap.SnapshotEntry) error

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

func (s *ServiceAdaptorSnapshotWriter) Write(value *indexedmap.SnapshotEntry) error {
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

func (s *ServiceAdaptorSnapshotStreamWriter) Write(value *indexedmap.SnapshotEntry) error {
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
	// Size returns the size of the map
	Size() (*indexedmap.SizeOutput, error)
	// Put puts an entry into the map
	Put(*indexedmap.PutInput) (*indexedmap.PutOutput, error)
	// Get gets the entry for a key
	Get(*indexedmap.GetInput) (*indexedmap.GetOutput, error)
	// FirstEntry gets the first entry in the map
	FirstEntry() (*indexedmap.FirstEntryOutput, error)
	// LastEntry gets the last entry in the map
	LastEntry() (*indexedmap.LastEntryOutput, error)
	// PrevEntry gets the previous entry in the map
	PrevEntry(*indexedmap.PrevEntryInput) (*indexedmap.PrevEntryOutput, error)
	// NextEntry gets the next entry in the map
	NextEntry(*indexedmap.NextEntryInput) (*indexedmap.NextEntryOutput, error)
	// Remove removes an entry from the map
	Remove(*indexedmap.RemoveInput) (*indexedmap.RemoveOutput, error)
	// Clear removes all entries from the map
	Clear() error
	// Events listens for change events
	Events(*indexedmap.EventsInput, ServiceEventsStream) (rsm.StreamCloser, error)
	// Entries lists all entries in the map
	Entries(*indexedmap.EntriesInput, ServiceEntriesStream) (rsm.StreamCloser, error)
	// Snapshot exports a snapshot of the primitive state
	Snapshot(ServiceSnapshotWriter) error
	// Restore imports a snapshot of the primitive state
	Restore(*indexedmap.SnapshotEntry) error
}
