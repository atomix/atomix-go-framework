package _map

import (
	_map "github.com/atomix/api/go/atomix/primitive/map"
	rsm "github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
	proto "github.com/golang/protobuf/proto"
)

type ServiceEventsStream interface {
	// ID returns the stream identifier
	ID() rsm.StreamID

	// OperationID returns the stream operation identifier
	OperationID() rsm.OperationID

	// Session returns the stream session
	Session() rsm.Session

	// Notify sends a value on the stream
	Notify(value *_map.EventsResponse) error

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

func (s *ServiceAdaptorEventsStream) Notify(value *_map.EventsResponse) error {
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
	Notify(value *_map.EntriesResponse) error

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

func (s *ServiceAdaptorEntriesStream) Notify(value *_map.EntriesResponse) error {
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

type Service interface {
	// Size returns the size of the map
	Size(*_map.SizeRequest) (*_map.SizeResponse, error)
	// Put puts an entry into the map
	Put(*_map.PutRequest) (*_map.PutResponse, error)
	// Get gets the entry for a key
	Get(*_map.GetRequest) (*_map.GetResponse, error)
	// Remove removes an entry from the map
	Remove(*_map.RemoveRequest) (*_map.RemoveResponse, error)
	// Clear removes all entries from the map
	Clear(*_map.ClearRequest) (*_map.ClearResponse, error)
	// Events listens for change events
	Events(*_map.EventsRequest, ServiceEventsStream) (rsm.StreamCloser, error)
	// Entries lists all entries in the map
	Entries(*_map.EntriesRequest, ServiceEntriesStream) (rsm.StreamCloser, error)
}
