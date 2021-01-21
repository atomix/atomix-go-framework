package indexedmap

import (
	indexedmap "github.com/atomix/api/go/atomix/primitive/indexedmap"
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
	Notify(value *indexedmap.EventsResponse) error

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

func (s *ServiceAdaptorEventsStream) Notify(value *indexedmap.EventsResponse) error {
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
	Notify(value *indexedmap.EntriesResponse) error

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

func (s *ServiceAdaptorEntriesStream) Notify(value *indexedmap.EntriesResponse) error {
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
	Size(*indexedmap.SizeRequest) (*indexedmap.SizeResponse, error)
	// Put puts an entry into the map
	Put(*indexedmap.PutRequest) (*indexedmap.PutResponse, error)
	// Get gets the entry for a key
	Get(*indexedmap.GetRequest) (*indexedmap.GetResponse, error)
	// FirstEntry gets the first entry in the map
	FirstEntry(*indexedmap.FirstEntryRequest) (*indexedmap.FirstEntryResponse, error)
	// LastEntry gets the last entry in the map
	LastEntry(*indexedmap.LastEntryRequest) (*indexedmap.LastEntryResponse, error)
	// PrevEntry gets the previous entry in the map
	PrevEntry(*indexedmap.PrevEntryRequest) (*indexedmap.PrevEntryResponse, error)
	// NextEntry gets the next entry in the map
	NextEntry(*indexedmap.NextEntryRequest) (*indexedmap.NextEntryResponse, error)
	// Remove removes an entry from the map
	Remove(*indexedmap.RemoveRequest) (*indexedmap.RemoveResponse, error)
	// Clear removes all entries from the map
	Clear(*indexedmap.ClearRequest) (*indexedmap.ClearResponse, error)
	// Events listens for change events
	Events(*indexedmap.EventsRequest, ServiceEventsStream) (rsm.StreamCloser, error)
	// Entries lists all entries in the map
	Entries(*indexedmap.EntriesRequest, ServiceEntriesStream) (rsm.StreamCloser, error)
}
