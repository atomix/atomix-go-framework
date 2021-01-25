

package leader

import (
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/golang/protobuf/proto"
	leader "github.com/atomix/api/go/atomix/primitive/leader"
)

const Type = "LeaderLatch"

const (
    latchOp = "Latch"
    getOp = "Get"
    eventsOp = "Events"
)

var newServiceFunc rsm.NewServiceFunc

func registerServiceFunc(rsmf NewServiceFunc) {
	newServiceFunc = func(scheduler rsm.Scheduler, context rsm.ServiceContext) rsm.Service {
		service := &ServiceAdaptor{
			Service: rsm.NewService(scheduler, context),
			rsm:     rsmf(scheduler, context),
			log:     logging.GetLogger("atomix", "leaderlatch", "service"),
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
	s.RegisterUnaryOperation(latchOp, s.latch)
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

func (s *ServiceAdaptor) latch(input []byte) ([]byte, error) {
    request := &leader.LatchRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.Latch(request)
	if err !=  nil {
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
    request := &leader.GetRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.Get(request)
	if err !=  nil {
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
    request := &leader.EventsRequest{}
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
