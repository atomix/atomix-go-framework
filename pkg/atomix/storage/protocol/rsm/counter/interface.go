// Code generated by atomix-go-framework. DO NOT EDIT.
package counter

import (
	counter "github.com/atomix/atomix-api/go/atomix/primitive/counter"
	errors "github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	rsm "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	uuid "github.com/google/uuid"
)

type Service interface {
	ServiceContext
	GetState() (*CounterState, error)
	SetState(*CounterState) error
	// Set sets the counter value
	Set(SetProposal) error
	// Get gets the current counter value
	Get(GetProposal) error
	// Increment increments the counter value
	Increment(IncrementProposal) error
	// Decrement decrements the counter value
	Decrement(DecrementProposal) error
}

type ServiceContext interface {
	Scheduler() rsm.Scheduler
	Sessions() Sessions
	Proposals() Proposals
}

func newServiceContext(scheduler rsm.Scheduler) ServiceContext {
	return &serviceContext{
		scheduler: scheduler,
		sessions:  newSessions(),
		proposals: newProposals(),
	}
}

type serviceContext struct {
	scheduler rsm.Scheduler
	sessions  Sessions
	proposals Proposals
}

func (s *serviceContext) Scheduler() rsm.Scheduler {
	return s.scheduler
}

func (s *serviceContext) Sessions() Sessions {
	return s.sessions
}

func (s *serviceContext) Proposals() Proposals {
	return s.proposals
}

var _ ServiceContext = &serviceContext{}

type Sessions interface {
	open(Session)
	expire(SessionID)
	close(SessionID)
	Get(SessionID) (Session, bool)
	List() []Session
}

func newSessions() Sessions {
	return &serviceSessions{
		sessions: make(map[SessionID]Session),
	}
}

type serviceSessions struct {
	sessions map[SessionID]Session
}

func (s *serviceSessions) open(session Session) {
	s.sessions[session.ID()] = session
	session.setState(SessionOpen)
}

func (s *serviceSessions) expire(sessionID SessionID) {
	session, ok := s.sessions[sessionID]
	if ok {
		session.setState(SessionClosed)
		delete(s.sessions, sessionID)
	}
}

func (s *serviceSessions) close(sessionID SessionID) {
	session, ok := s.sessions[sessionID]
	if ok {
		session.setState(SessionClosed)
		delete(s.sessions, sessionID)
	}
}

func (s *serviceSessions) Get(id SessionID) (Session, bool) {
	session, ok := s.sessions[id]
	return session, ok
}

func (s *serviceSessions) List() []Session {
	sessions := make([]Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}

var _ Sessions = &serviceSessions{}

type SessionID uint64

type SessionState int

const (
	SessionClosed SessionState = iota
	SessionOpen
)

type Watcher interface {
	Cancel()
}

func newWatcher(f func()) Watcher {
	return &serviceWatcher{
		f: f,
	}
}

type serviceWatcher struct {
	f func()
}

func (s *serviceWatcher) Cancel() {
	s.f()
}

var _ Watcher = &serviceWatcher{}

type Session interface {
	ID() SessionID
	State() SessionState
	setState(SessionState)
	Watch(func(SessionState)) Watcher
	Proposals() Proposals
}

func newSession(session rsm.Session) Session {
	return &serviceSession{
		session:   session,
		proposals: newProposals(),
		watchers:  make(map[string]func(SessionState)),
	}
}

type serviceSession struct {
	session   rsm.Session
	proposals Proposals
	state     SessionState
	watchers  map[string]func(SessionState)
}

func (s *serviceSession) ID() SessionID {
	return SessionID(s.session.ID())
}

func (s *serviceSession) Proposals() Proposals {
	return s.proposals
}

func (s *serviceSession) State() SessionState {
	return s.state
}

func (s *serviceSession) setState(state SessionState) {
	if state != s.state {
		s.state = state
		for _, watcher := range s.watchers {
			watcher(state)
		}
	}
}

func (s *serviceSession) Watch(f func(SessionState)) Watcher {
	id := uuid.New().String()
	s.watchers[id] = f
	return newWatcher(func() {
		delete(s.watchers, id)
	})
}

var _ Session = &serviceSession{}

type Proposals interface {
	Set() SetProposals
	Get() GetProposals
	Increment() IncrementProposals
	Decrement() DecrementProposals
}

func newProposals() Proposals {
	return &serviceProposals{
		setProposals:       newSetProposals(),
		getProposals:       newGetProposals(),
		incrementProposals: newIncrementProposals(),
		decrementProposals: newDecrementProposals(),
	}
}

type serviceProposals struct {
	setProposals       SetProposals
	getProposals       GetProposals
	incrementProposals IncrementProposals
	decrementProposals DecrementProposals
}

func (s *serviceProposals) Set() SetProposals {
	return s.setProposals
}
func (s *serviceProposals) Get() GetProposals {
	return s.getProposals
}
func (s *serviceProposals) Increment() IncrementProposals {
	return s.incrementProposals
}
func (s *serviceProposals) Decrement() DecrementProposals {
	return s.decrementProposals
}

var _ Proposals = &serviceProposals{}

type ProposalID uint64

type Proposal interface {
	ID() ProposalID
	Session() Session
}

func newProposal(id ProposalID, session Session) Proposal {
	return &serviceProposal{
		id:      id,
		session: session,
	}
}

type serviceProposal struct {
	id      ProposalID
	session Session
}

func (p *serviceProposal) ID() ProposalID {
	return p.id
}

func (p *serviceProposal) Session() Session {
	return p.session
}

var _ Proposal = &serviceProposal{}

type SetProposals interface {
	register(SetProposal)
	unregister(ProposalID)
	Get(ProposalID) (SetProposal, bool)
	List() []SetProposal
}

func newSetProposals() SetProposals {
	return &setProposals{
		proposals: make(map[ProposalID]SetProposal),
	}
}

type setProposals struct {
	proposals map[ProposalID]SetProposal
}

func (p *setProposals) register(proposal SetProposal) {
	p.proposals[proposal.ID()] = proposal
}

func (p *setProposals) unregister(id ProposalID) {
	delete(p.proposals, id)
}

func (p *setProposals) Get(id ProposalID) (SetProposal, bool) {
	proposal, ok := p.proposals[id]
	return proposal, ok
}

func (p *setProposals) List() []SetProposal {
	proposals := make([]SetProposal, 0, len(p.proposals))
	for _, proposal := range p.proposals {
		proposals = append(proposals, proposal)
	}
	return proposals
}

var _ SetProposals = &setProposals{}

type SetProposal interface {
	Proposal
	Request() *counter.SetRequest
	Reply(*counter.SetResponse) error
}

func newSetProposal(id ProposalID, session Session, request *counter.SetRequest, response *counter.SetResponse) SetProposal {
	return &setProposal{
		Proposal: newProposal(id, session),
		request:  request,
		response: response,
	}
}

type setProposal struct {
	Proposal
	request  *counter.SetRequest
	response *counter.SetResponse
}

func (p *setProposal) Request() *counter.SetRequest {
	return p.request
}

func (p *setProposal) Reply(reply *counter.SetResponse) error {
	if p.response != nil {
		return errors.NewConflict("reply already sent")
	}
	p.response = reply
	return nil
}

var _ SetProposal = &setProposal{}

type GetProposals interface {
	register(GetProposal)
	unregister(ProposalID)
	Get(ProposalID) (GetProposal, bool)
	List() []GetProposal
}

func newGetProposals() GetProposals {
	return &getProposals{
		proposals: make(map[ProposalID]GetProposal),
	}
}

type getProposals struct {
	proposals map[ProposalID]GetProposal
}

func (p *getProposals) register(proposal GetProposal) {
	p.proposals[proposal.ID()] = proposal
}

func (p *getProposals) unregister(id ProposalID) {
	delete(p.proposals, id)
}

func (p *getProposals) Get(id ProposalID) (GetProposal, bool) {
	proposal, ok := p.proposals[id]
	return proposal, ok
}

func (p *getProposals) List() []GetProposal {
	proposals := make([]GetProposal, 0, len(p.proposals))
	for _, proposal := range p.proposals {
		proposals = append(proposals, proposal)
	}
	return proposals
}

var _ GetProposals = &getProposals{}

type GetProposal interface {
	Proposal
	Request() *counter.GetRequest
	Reply(*counter.GetResponse) error
}

func newGetProposal(id ProposalID, session Session, request *counter.GetRequest, response *counter.GetResponse) GetProposal {
	return &getProposal{
		Proposal: newProposal(id, session),
		request:  request,
		response: response,
	}
}

type getProposal struct {
	Proposal
	request  *counter.GetRequest
	response *counter.GetResponse
}

func (p *getProposal) Request() *counter.GetRequest {
	return p.request
}

func (p *getProposal) Reply(reply *counter.GetResponse) error {
	if p.response != nil {
		return errors.NewConflict("reply already sent")
	}
	p.response = reply
	return nil
}

var _ GetProposal = &getProposal{}

type IncrementProposals interface {
	register(IncrementProposal)
	unregister(ProposalID)
	Get(ProposalID) (IncrementProposal, bool)
	List() []IncrementProposal
}

func newIncrementProposals() IncrementProposals {
	return &incrementProposals{
		proposals: make(map[ProposalID]IncrementProposal),
	}
}

type incrementProposals struct {
	proposals map[ProposalID]IncrementProposal
}

func (p *incrementProposals) register(proposal IncrementProposal) {
	p.proposals[proposal.ID()] = proposal
}

func (p *incrementProposals) unregister(id ProposalID) {
	delete(p.proposals, id)
}

func (p *incrementProposals) Get(id ProposalID) (IncrementProposal, bool) {
	proposal, ok := p.proposals[id]
	return proposal, ok
}

func (p *incrementProposals) List() []IncrementProposal {
	proposals := make([]IncrementProposal, 0, len(p.proposals))
	for _, proposal := range p.proposals {
		proposals = append(proposals, proposal)
	}
	return proposals
}

var _ IncrementProposals = &incrementProposals{}

type IncrementProposal interface {
	Proposal
	Request() *counter.IncrementRequest
	Reply(*counter.IncrementResponse) error
}

func newIncrementProposal(id ProposalID, session Session, request *counter.IncrementRequest, response *counter.IncrementResponse) IncrementProposal {
	return &incrementProposal{
		Proposal: newProposal(id, session),
		request:  request,
		response: response,
	}
}

type incrementProposal struct {
	Proposal
	request  *counter.IncrementRequest
	response *counter.IncrementResponse
}

func (p *incrementProposal) Request() *counter.IncrementRequest {
	return p.request
}

func (p *incrementProposal) Reply(reply *counter.IncrementResponse) error {
	if p.response != nil {
		return errors.NewConflict("reply already sent")
	}
	p.response = reply
	return nil
}

var _ IncrementProposal = &incrementProposal{}

type DecrementProposals interface {
	register(DecrementProposal)
	unregister(ProposalID)
	Get(ProposalID) (DecrementProposal, bool)
	List() []DecrementProposal
}

func newDecrementProposals() DecrementProposals {
	return &decrementProposals{
		proposals: make(map[ProposalID]DecrementProposal),
	}
}

type decrementProposals struct {
	proposals map[ProposalID]DecrementProposal
}

func (p *decrementProposals) register(proposal DecrementProposal) {
	p.proposals[proposal.ID()] = proposal
}

func (p *decrementProposals) unregister(id ProposalID) {
	delete(p.proposals, id)
}

func (p *decrementProposals) Get(id ProposalID) (DecrementProposal, bool) {
	proposal, ok := p.proposals[id]
	return proposal, ok
}

func (p *decrementProposals) List() []DecrementProposal {
	proposals := make([]DecrementProposal, 0, len(p.proposals))
	for _, proposal := range p.proposals {
		proposals = append(proposals, proposal)
	}
	return proposals
}

var _ DecrementProposals = &decrementProposals{}

type DecrementProposal interface {
	Proposal
	Request() *counter.DecrementRequest
	Reply(*counter.DecrementResponse) error
}

func newDecrementProposal(id ProposalID, session Session, request *counter.DecrementRequest, response *counter.DecrementResponse) DecrementProposal {
	return &decrementProposal{
		Proposal: newProposal(id, session),
		request:  request,
		response: response,
	}
}

type decrementProposal struct {
	Proposal
	request  *counter.DecrementRequest
	response *counter.DecrementResponse
}

func (p *decrementProposal) Request() *counter.DecrementRequest {
	return p.request
}

func (p *decrementProposal) Reply(reply *counter.DecrementResponse) error {
	if p.response != nil {
		return errors.NewConflict("reply already sent")
	}
	p.response = reply
	return nil
}

var _ DecrementProposal = &decrementProposal{}
