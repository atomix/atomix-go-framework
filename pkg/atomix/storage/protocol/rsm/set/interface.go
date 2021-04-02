

package set

import (
	set "github.com/atomix/api/go/atomix/primitive/set"
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
	Notify(value *set.EventsResponse) error

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

func (s *ServiceAdaptorEventsStream) Notify(value *set.EventsResponse) error {
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
	Notify(value *set.ElementsResponse) error

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

func (s *ServiceAdaptorElementsStream) Notify(value *set.ElementsResponse) error {
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
    // Size gets the number of elements in the set
    Size(*set.SizeRequest) (*set.SizeResponse, error)
    // Contains returns whether the set contains a value
    Contains(*set.ContainsRequest) (*set.ContainsResponse, error)
    // Add adds a value to the set
    Add(*set.AddRequest) (*set.AddResponse, error)
    // Remove removes a value from the set
    Remove(*set.RemoveRequest) (*set.RemoveResponse, error)
    // Clear removes all values from the set
    Clear(*set.ClearRequest) (*set.ClearResponse, error)
    // Events listens for set change events
    Events(*set.EventsRequest, ServiceEventsStream) (rsm.StreamCloser, error)
    // Elements lists all elements in the set
    Elements(*set.ElementsRequest, ServiceElementsStream) (rsm.StreamCloser, error)
}
