package log

import (
	log "github.com/atomix/api/go/atomix/primitive/log"
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
	Notify(value *log.EventsOutput) error

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

func (s *ServiceAdaptorEventsStream) Notify(value *log.EventsOutput) error {
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
	Notify(value *log.EntriesOutput) error

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

func (s *ServiceAdaptorEntriesStream) Notify(value *log.EntriesOutput) error {
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
	Write(value *log.SnapshotEntry) error

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

func (s *ServiceAdaptorSnapshotWriter) Write(value *log.SnapshotEntry) error {
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

func (s *ServiceAdaptorSnapshotStreamWriter) Write(value *log.SnapshotEntry) error {
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
	// Size returns the size of the log
	Size() (*log.SizeOutput, error)
	// Exists checks whether an index exists in the log
	Exists(*log.ExistsInput) (*log.ExistsOutput, error)
	// Appends appends an entry into the log
	Append(*log.AppendInput) (*log.AppendOutput, error)
	// Get gets the entry for an index
	Get(*log.GetInput) (*log.GetOutput, error)
	// FirstEntry gets the first entry in the log
	FirstEntry() (*log.FirstEntryOutput, error)
	// LastEntry gets the last entry in the log
	LastEntry() (*log.LastEntryOutput, error)
	// PrevEntry gets the previous entry in the log
	PrevEntry(*log.PrevEntryInput) (*log.PrevEntryOutput, error)
	// NextEntry gets the next entry in the log
	NextEntry(*log.NextEntryInput) (*log.NextEntryOutput, error)
	// Remove removes an entry from the log
	Remove(*log.RemoveInput) (*log.RemoveOutput, error)
	// Clear removes all entries from the log
	Clear() error
	// Events listens for change events
	Events(*log.EventsInput, ServiceEventsStream) error
	// Entries lists all entries in the log
	Entries(*log.EntriesInput, ServiceEntriesStream) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot(ServiceSnapshotWriter) error
	// Restore imports a snapshot of the primitive state
	Restore(*log.SnapshotEntry) error
}