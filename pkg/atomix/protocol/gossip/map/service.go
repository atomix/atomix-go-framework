package _map

import (
	"context"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/golang/protobuf/proto"
	"io"
)

const ServiceType gossip.ServiceType = "Map"

// RegisterService registers the service on the given node
func RegisterService(node *gossip.Node) {
	node.RegisterService(ServiceType, newServiceFunc)
}

var newServiceFunc gossip.NewServiceFunc

type ServiceEventsStream interface {
	// Notify sends a value on the stream
	Notify(value *_map.EventsOutput) error

	// Close closes the stream
	Close()
}

func newServiceEventsStream(stream streams.WriteStream) ServiceEventsStream {
	return &ServiceAdaptorEventsStream{
		stream: stream,
	}
}

type ServiceAdaptorEventsStream struct {
	stream streams.WriteStream
}

func (s *ServiceAdaptorEventsStream) Notify(value *_map.EventsOutput) error {
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
	// Notify sends a value on the stream
	Notify(value *_map.EntriesOutput) error

	// Close closes the stream
	Close()
}

func newServiceEntriesStream(stream streams.WriteStream) ServiceEntriesStream {
	return &ServiceAdaptorEntriesStream{
		stream: stream,
	}
}

type ServiceAdaptorEntriesStream struct {
	stream streams.WriteStream
}

func (s *ServiceAdaptorEntriesStream) Notify(value *_map.EntriesOutput) error {
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
	Write(value *_map.SnapshotEntry) error

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

func (s *ServiceAdaptorSnapshotWriter) Write(value *_map.SnapshotEntry) error {
	bytes, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	return util.WriteBytes(s.writer, bytes)
}

func (s *ServiceAdaptorSnapshotWriter) Close() {

}

var _ ServiceSnapshotWriter = &ServiceAdaptorSnapshotWriter{}

func newServiceSnapshotStreamWriter(stream streams.WriteStream) ServiceSnapshotWriter {
	return &ServiceAdaptorSnapshotStreamWriter{
		stream: stream,
	}
}

type ServiceAdaptorSnapshotStreamWriter struct {
	stream streams.WriteStream
}

func (s *ServiceAdaptorSnapshotStreamWriter) Write(value *_map.SnapshotEntry) error {
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
	gossip.Service
	// Size returns the size of the map
	Size(context.Context) (*_map.SizeOutput, error)
	// Put puts an entry into the map
	Put(context.Context, *_map.PutInput) (*_map.PutOutput, error)
	// Get gets the entry for a key
	Get(context.Context, *_map.GetInput) (*_map.GetOutput, error)
	// Remove removes an entry from the map
	Remove(context.Context, *_map.RemoveInput) (*_map.RemoveOutput, error)
	// Clear removes all entries from the map
	Clear(context.Context) error
	// Events listens for change events
	Events(context.Context, *_map.EventsInput, ServiceEventsStream) error
	// Entries lists all entries in the map
	Entries(context.Context, *_map.EntriesInput, ServiceEntriesStream) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context, ServiceSnapshotWriter) error
	// Restore imports a snapshot of the primitive state
	Restore(context.Context, *_map.SnapshotEntry) error
}
