package value

import (
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/golang/protobuf/proto"
)

const Type = "Value"

const (
	setOp    = "Set"
	getOp    = "Get"
	eventsOp = "Events"
)

var newServiceFunc rsm.NewServiceFunc

func registerServiceFunc(rsmf NewServiceFunc) {
	newServiceFunc = func(scheduler rsm.Scheduler, context rsm.ServiceContext) rsm.Service {
		service := &ServiceAdaptor{
			Service: rsm.NewService(scheduler, context),
			rsm:     rsmf(scheduler, context),
			log:     logging.GetLogger("atomix", "value", "service"),
		}
		service.init()
		return service
	}
}

type NewServiceFunc func(scheduler rsm.Scheduler, context rsm.ServiceContext) Service

// RegisterService registers the election primitive service on the given node
func RegisterService(node *rsm.Node) {
	node.RegisterService(Type, newServiceFunc)
}

type ServiceAdaptor struct {
	rsm.Service
	rsm Service
	log logging.Logger
}

func (s *ServiceAdaptor) init() {
	s.RegisterUnaryOperation(setOp, s.set)
	s.RegisterUnaryOperation(getOp, s.get)
	s.RegisterStreamOperation(eventsOp, s.events)
}

func (s *ServiceAdaptor) SessionOpen(session rsm.Session) {
	if sessionOpen, ok := s.rsm.(rsm.SessionOpenService); ok {
		sessionOpen.SessionOpen(session)
	}
}

func (s *ServiceAdaptor) SessionExpired(session rsm.Session) {
	if sessionExpired, ok := s.rsm.(rsm.SessionExpiredService); ok {
		sessionExpired.SessionExpired(session)
	}
}

func (s *ServiceAdaptor) SessionClosed(session rsm.Session) {
	if sessionClosed, ok := s.rsm.(rsm.SessionClosedService); ok {
		sessionClosed.SessionClosed(session)
	}
}

func (s *ServiceAdaptor) set(input []byte) ([]byte, error) {
	request := &value.SetRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.Set(request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	output, err := proto.Marshal(response)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return output, nil
}

func (s *ServiceAdaptor) get(input []byte) ([]byte, error) {
	request := &value.GetRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.Get(request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	output, err := proto.Marshal(response)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return output, nil
}

func (s *ServiceAdaptor) events(input []byte, stream rsm.Stream) (rsm.StreamCloser, error) {
	request := &value.EventsRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	response := newServiceEventsStream(stream)
	closer, err := s.rsm.Events(request, response)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return closer, nil
}

var _ rsm.Service = &ServiceAdaptor{}
