package election

import (
	election "github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/golang/protobuf/proto"
	"io"
)

const Type = "Election"

const (
	enterOp    = "Enter"
	withdrawOp = "Withdraw"
	anointOp   = "Anoint"
	promoteOp  = "Promote"
	evictOp    = "Evict"
	getTermOp  = "GetTerm"
	eventsOp   = "Events"
	snapshotOp = "Snapshot"
	restoreOp  = "Restore"
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
	s.RegisterUnaryOperation(snapshotOp, s.snapshot)
	s.RegisterUnaryOperation(restoreOp, s.restore)
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

func (s *ServiceAdaptor) Backup(writer io.Writer) error {
	snapshot, err := s.rsm.Snapshot()
	if err != nil {
		s.log.Error(err)
		return err
	}
	bytes, err := proto.Marshal(snapshot)
	if err != nil {
		s.log.Error(err)
		return err
	}
	return util.WriteBytes(writer, bytes)
}

func (s *ServiceAdaptor) Restore(reader io.Reader) error {
	bytes, err := util.ReadBytes(reader)
	if err != nil {
		s.log.Error(err)
		return err
	}
	snapshot := &election.Snapshot{}
	err = proto.Unmarshal(bytes, snapshot)
	if err != nil {
		return err
	}
	err = s.rsm.Restore(snapshot)
	if err != nil {
		s.log.Error(err)
		return err
	}
	return nil
}

func (s *ServiceAdaptor) enter(in []byte) ([]byte, error) {
	input := &election.EnterInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	output, err := s.rsm.Enter(input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	out, err := proto.Marshal(output)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return out, nil
}

func (s *ServiceAdaptor) withdraw(in []byte) ([]byte, error) {
	input := &election.WithdrawInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	output, err := s.rsm.Withdraw(input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	out, err := proto.Marshal(output)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return out, nil
}

func (s *ServiceAdaptor) anoint(in []byte) ([]byte, error) {
	input := &election.AnointInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	output, err := s.rsm.Anoint(input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	out, err := proto.Marshal(output)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return out, nil
}

func (s *ServiceAdaptor) promote(in []byte) ([]byte, error) {
	input := &election.PromoteInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	output, err := s.rsm.Promote(input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	out, err := proto.Marshal(output)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return out, nil
}

func (s *ServiceAdaptor) evict(in []byte) ([]byte, error) {
	input := &election.EvictInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	output, err := s.rsm.Evict(input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	out, err := proto.Marshal(output)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return out, nil
}

func (s *ServiceAdaptor) getTerm(in []byte) ([]byte, error) {
	input := &election.GetTermInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	output, err := s.rsm.GetTerm(input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	out, err := proto.Marshal(output)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return out, nil
}

func (s *ServiceAdaptor) events(in []byte, stream rsm.Stream) {
	input := &election.EventsInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		stream.Error(err)
		stream.Close()
		return
	}
	output := newServiceEventsStream(stream)
	err = s.rsm.Events(input, output)
	if err != nil {
		s.log.Error(err)
		stream.Error(err)
		stream.Close()
		return
	}
}

func (s *ServiceAdaptor) snapshot(in []byte) ([]byte, error) {
	output, err := s.rsm.Snapshot()
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	out, err := proto.Marshal(output)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return out, nil
}

func (s *ServiceAdaptor) restore(in []byte) ([]byte, error) {
	input := &election.Snapshot{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	err = s.rsm.Restore(input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return nil, nil
}

var _ rsm.Service = &ServiceAdaptor{}
