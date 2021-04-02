

package log

import (
	log "github.com/atomix/api/go/atomix/primitive/log"
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
	Notify(value *log.EventsResponse) error

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

func (s *ServiceAdaptorEventsStream) Notify(value *log.EventsResponse) error {
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
	Notify(value *log.EntriesResponse) error

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

func (s *ServiceAdaptorEntriesStream) Notify(value *log.EntriesResponse) error {
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
    // Size returns the size of the log
    Size(*log.SizeRequest) (*log.SizeResponse, error)
    // Appends appends an entry into the log
    Append(*log.AppendRequest) (*log.AppendResponse, error)
    // Get gets the entry for an index
    Get(*log.GetRequest) (*log.GetResponse, error)
    // FirstEntry gets the first entry in the log
    FirstEntry(*log.FirstEntryRequest) (*log.FirstEntryResponse, error)
    // LastEntry gets the last entry in the log
    LastEntry(*log.LastEntryRequest) (*log.LastEntryResponse, error)
    // PrevEntry gets the previous entry in the log
    PrevEntry(*log.PrevEntryRequest) (*log.PrevEntryResponse, error)
    // NextEntry gets the next entry in the log
    NextEntry(*log.NextEntryRequest) (*log.NextEntryResponse, error)
    // Remove removes an entry from the log
    Remove(*log.RemoveRequest) (*log.RemoveResponse, error)
    // Clear removes all entries from the log
    Clear(*log.ClearRequest) (*log.ClearResponse, error)
    // Events listens for change events
    Events(*log.EventsRequest, ServiceEventsStream) (rsm.StreamCloser, error)
    // Entries lists all entries in the log
    Entries(*log.EntriesRequest, ServiceEntriesStream) (rsm.StreamCloser, error)
}
