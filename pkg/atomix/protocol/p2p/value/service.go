package value

import (
	"context"
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/protocol/p2p"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
)

const ServiceType p2p.ServiceType = "Value"

// RegisterService registers the service on the given node
func RegisterService(node *p2p.Node) {
	node.RegisterService(ServiceType, newServiceFunc)
}

var newServiceFunc p2p.NewServiceFunc

type ServiceEventsStream interface {
	// Notify sends a value on the stream
	Notify(value *value.EventsOutput) error

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

func (s *ServiceAdaptorEventsStream) Notify(value *value.EventsOutput) error {
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

type Service interface {
	p2p.Service
	// Set sets the value
	Set(context.Context, *value.SetInput) (*value.SetOutput, error)
	// Get gets the value
	Get(context.Context, *value.GetInput) (*value.GetOutput, error)
	// Events listens for value change events
	Events(context.Context, *value.EventsInput, ServiceEventsStream) error
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context) (*value.Snapshot, error)
	// Restore imports a snapshot of the primitive state
	Restore(context.Context, *value.Snapshot) error
}
