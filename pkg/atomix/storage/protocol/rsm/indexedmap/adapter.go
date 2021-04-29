package indexedmap

import (
	indexedmap "github.com/atomix/atomix-api/go/atomix/primitive/indexedmap"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/golang/protobuf/proto"
)

const Type = "IndexedMap"

const (
	sizeOp       = "Size"
	putOp        = "Put"
	getOp        = "Get"
	firstEntryOp = "FirstEntry"
	lastEntryOp  = "LastEntry"
	prevEntryOp  = "PrevEntry"
	nextEntryOp  = "NextEntry"
	removeOp     = "Remove"
	clearOp      = "Clear"
	eventsOp     = "Events"
	entriesOp    = "Entries"
)

var newServiceFunc rsm.NewServiceFunc

func registerServiceFunc(rsmf NewServiceFunc) {
	newServiceFunc = func(scheduler rsm.Scheduler, context rsm.ServiceContext) rsm.Service {
		service := &ServiceAdaptor{
			Service: rsm.NewService(scheduler, context),
			rsm:     rsmf(scheduler, context),
			log:     logging.GetLogger("atomix", "indexedmap", "service"),
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
	s.RegisterUnaryOperation(sizeOp, s.size)
	s.RegisterUnaryOperation(putOp, s.put)
	s.RegisterUnaryOperation(getOp, s.get)
	s.RegisterUnaryOperation(firstEntryOp, s.firstEntry)
	s.RegisterUnaryOperation(lastEntryOp, s.lastEntry)
	s.RegisterUnaryOperation(prevEntryOp, s.prevEntry)
	s.RegisterUnaryOperation(nextEntryOp, s.nextEntry)
	s.RegisterUnaryOperation(removeOp, s.remove)
	s.RegisterUnaryOperation(clearOp, s.clear)
	s.RegisterStreamOperation(eventsOp, s.events)
	s.RegisterStreamOperation(entriesOp, s.entries)
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

func (s *ServiceAdaptor) size(input []byte) ([]byte, error) {
	request := &indexedmap.SizeRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.Size(request)
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

func (s *ServiceAdaptor) put(input []byte) ([]byte, error) {
	request := &indexedmap.PutRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.Put(request)
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
	request := &indexedmap.GetRequest{}
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

func (s *ServiceAdaptor) firstEntry(input []byte) ([]byte, error) {
	request := &indexedmap.FirstEntryRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.FirstEntry(request)
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

func (s *ServiceAdaptor) lastEntry(input []byte) ([]byte, error) {
	request := &indexedmap.LastEntryRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.LastEntry(request)
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

func (s *ServiceAdaptor) prevEntry(input []byte) ([]byte, error) {
	request := &indexedmap.PrevEntryRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.PrevEntry(request)
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

func (s *ServiceAdaptor) nextEntry(input []byte) ([]byte, error) {
	request := &indexedmap.NextEntryRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.NextEntry(request)
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

func (s *ServiceAdaptor) remove(input []byte) ([]byte, error) {
	request := &indexedmap.RemoveRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.Remove(request)
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

func (s *ServiceAdaptor) clear(input []byte) ([]byte, error) {
	request := &indexedmap.ClearRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.Clear(request)
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
	request := &indexedmap.EventsRequest{}
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

func (s *ServiceAdaptor) entries(input []byte, stream rsm.Stream) (rsm.StreamCloser, error) {
	request := &indexedmap.EntriesRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	response := newServiceEntriesStream(stream)
	closer, err := s.rsm.Entries(request, response)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return closer, nil
}

var _ rsm.Service = &ServiceAdaptor{}
