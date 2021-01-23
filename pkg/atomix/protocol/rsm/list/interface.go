package list

import (
	list "github.com/atomix/api/go/atomix/primitive/list"
	rsm "github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
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
	Notify(value *list.EventsResponse) error

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

func (s *ServiceAdaptorEventsStream) Notify(value *list.EventsResponse) error {
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
	Notify(value *list.ElementsResponse) error

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

func (s *ServiceAdaptorElementsStream) Notify(value *list.ElementsResponse) error {
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

type Service interface {
	// Size gets the number of elements in the list
	Size(*list.SizeRequest) (*list.SizeResponse, error)
	// Append appends a value to the list
	Append(*list.AppendRequest) (*list.AppendResponse, error)
	// Insert inserts a value at a specific index in the list
	Insert(*list.InsertRequest) (*list.InsertResponse, error)
	// Get gets the value at an index in the list
	Get(*list.GetRequest) (*list.GetResponse, error)
	// Set sets the value at an index in the list
	Set(*list.SetRequest) (*list.SetResponse, error)
	// Remove removes an element from the list
	Remove(*list.RemoveRequest) (*list.RemoveResponse, error)
	// Clear removes all elements from the list
	Clear(*list.ClearRequest) (*list.ClearResponse, error)
	// Events listens for change events
	Events(*list.EventsRequest, ServiceEventsStream) (rsm.StreamCloser, error)
	// Elements streams all elements in the list
	Elements(*list.ElementsRequest, ServiceElementsStream) (rsm.StreamCloser, error)
}
