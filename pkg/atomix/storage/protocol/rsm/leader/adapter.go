// Code generated by atomix-go-framework. DO NOT EDIT.
package leader

import (
	leader "github.com/atomix/atomix-api/go/atomix/primitive/leader"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/golang/protobuf/proto"
	"io"
)

var log = logging.GetLogger("atomix", "leaderlatch", "service")

const Type = "LeaderLatch"

const (
	latchOp  = "Latch"
	getOp    = "Get"
	eventsOp = "Events"
)

var newServiceFunc rsm.NewServiceFunc

func registerServiceFunc(rsmf NewServiceFunc) {
	newServiceFunc = func(scheduler rsm.Scheduler, context rsm.ServiceContext) rsm.Service {
		service := &ServiceAdaptor{
			Service: rsm.NewService(scheduler, context),
			rsm:     rsmf(newServiceContext(scheduler)),
		}
		service.init()
		return service
	}
}

type NewServiceFunc func(ServiceContext) Service

// RegisterService registers the election primitive service on the given node
func RegisterService(node *rsm.Node) {
	node.RegisterService(Type, newServiceFunc)
}

type ServiceAdaptor struct {
	rsm.Service
	rsm Service
}

func (s *ServiceAdaptor) init() {
	s.RegisterUnaryOperation(latchOp, s.latch)
	s.RegisterUnaryOperation(getOp, s.get)
	s.RegisterStreamOperation(eventsOp, s.events)
}
func (s *ServiceAdaptor) SessionOpen(rsmSession rsm.Session) {
	s.rsm.Sessions().open(newSession(rsmSession))
}

func (s *ServiceAdaptor) SessionExpired(session rsm.Session) {
	s.rsm.Sessions().expire(SessionID(session.ID()))
}

func (s *ServiceAdaptor) SessionClosed(session rsm.Session) {
	s.rsm.Sessions().close(SessionID(session.ID()))
}
func (s *ServiceAdaptor) Backup(writer io.Writer) error {
	err := s.rsm.Backup(newSnapshotWriter(writer))
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (s *ServiceAdaptor) Restore(reader io.Reader) error {
	err := s.rsm.Restore(newSnapshotReader(reader))
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}
func (s *ServiceAdaptor) latch(input []byte, rsmSession rsm.Session) ([]byte, error) {
	request := &leader.LatchRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	session, ok := s.rsm.Sessions().Get(SessionID(rsmSession.ID()))
	if !ok {
		err := errors.NewConflict("session %d not found", rsmSession.ID())
		log.Error(err.Error())
		return nil, err
	}

	proposal := newLatchProposal(ProposalID(s.Index()), session, request)

	s.rsm.Proposals().Latch().register(proposal)
	session.Proposals().Latch().register(proposal)

	defer func() {
		session.Proposals().Latch().unregister(proposal.ID())
		s.rsm.Proposals().Latch().unregister(proposal.ID())
	}()

	log.Debugf("Proposing LatchProposal %s", proposal)
	err = s.rsm.Latch(proposal)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	output, err := proto.Marshal(proposal.response())
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return output, nil
}
func (s *ServiceAdaptor) get(input []byte, rsmSession rsm.Session) ([]byte, error) {
	request := &leader.GetRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	session, ok := s.rsm.Sessions().Get(SessionID(rsmSession.ID()))
	if !ok {
		err := errors.NewConflict("session %d not found", rsmSession.ID())
		log.Error(err.Error())
		return nil, err
	}

	proposal := newGetProposal(ProposalID(s.Index()), session, request)

	s.rsm.Proposals().Get().register(proposal)
	session.Proposals().Get().register(proposal)

	defer func() {
		session.Proposals().Get().unregister(proposal.ID())
		s.rsm.Proposals().Get().unregister(proposal.ID())
	}()

	log.Debugf("Proposing GetProposal %s", proposal)
	err = s.rsm.Get(proposal)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	output, err := proto.Marshal(proposal.response())
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return output, nil
}
func (s *ServiceAdaptor) events(input []byte, rsmSession rsm.Session, stream rsm.Stream) (rsm.StreamCloser, error) {
	request := &leader.EventsRequest{}
	err := proto.Unmarshal(input, request)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	session, ok := s.rsm.Sessions().Get(SessionID(rsmSession.ID()))
	if !ok {
		err := errors.NewConflict("session %d not found", rsmSession.ID())
		log.Error(err.Error())
		return nil, err
	}

	proposal := newEventsProposal(ProposalID(stream.ID()), session, request, stream)

	s.rsm.Proposals().Events().register(proposal)
	session.Proposals().Events().register(proposal)

	log.Debugf("Proposing EventsProposal %s", proposal)
	err = s.rsm.Events(proposal)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	return func() {
		session.Proposals().Events().unregister(proposal.ID())
		s.rsm.Proposals().Events().unregister(proposal.ID())
	}, nil
}

var _ rsm.Service = &ServiceAdaptor{}
