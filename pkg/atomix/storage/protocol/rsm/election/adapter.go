

package election

import (
	"github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/golang/protobuf/proto"
	election "github.com/atomix/api/go/atomix/primitive/election"
)

const Type = "Election"

const (
    enterOp = "Enter"
    withdrawOp = "Withdraw"
    anointOp = "Anoint"
    promoteOp = "Promote"
    evictOp = "Evict"
    getTermOp = "GetTerm"
    eventsOp = "Events"
)

var newServiceFunc rsm.NewServiceFunc

func registerServiceFunc(rsmf NewServiceFunc) {
	newServiceFunc = func(scheduler rsm.Scheduler, context rsm.ServiceContext) rsm.Service {
		service := &ServiceAdaptor{
			Service: rsm.NewService(scheduler, context),
			rsm:     rsmf(scheduler, context),
			log:     logging.GetLogger("atomix", "election", "service"),
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
	s.RegisterUnaryOperation(enterOp, s.enter)
	s.RegisterUnaryOperation(withdrawOp, s.withdraw)
	s.RegisterUnaryOperation(anointOp, s.anoint)
	s.RegisterUnaryOperation(promoteOp, s.promote)
	s.RegisterUnaryOperation(evictOp, s.evict)
	s.RegisterUnaryOperation(getTermOp, s.getTerm)
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

func (s *ServiceAdaptor) enter(input []byte) ([]byte, error) {
    request := &election.EnterRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.Enter(request)
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


func (s *ServiceAdaptor) withdraw(input []byte) ([]byte, error) {
    request := &election.WithdrawRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.Withdraw(request)
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


func (s *ServiceAdaptor) anoint(input []byte) ([]byte, error) {
    request := &election.AnointRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.Anoint(request)
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


func (s *ServiceAdaptor) promote(input []byte) ([]byte, error) {
    request := &election.PromoteRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.Promote(request)
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


func (s *ServiceAdaptor) evict(input []byte) ([]byte, error) {
    request := &election.EvictRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.Evict(request)
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


func (s *ServiceAdaptor) getTerm(input []byte) ([]byte, error) {
    request := &election.GetTermRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.GetTerm(request)
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
    request := &election.EventsRequest{}
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
