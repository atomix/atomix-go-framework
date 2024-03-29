// Code generated by atomix-go-framework. DO NOT EDIT.

// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"fmt"
	election "github.com/atomix/atomix-api/go/atomix/primitive/election"
	errors "github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	rsm "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	util "github.com/atomix/atomix-go-framework/pkg/atomix/util"
	proto "github.com/golang/protobuf/proto"
	"io"
)

type Service interface {
	ServiceContext
	Backup(SnapshotWriter) error
	Restore(SnapshotReader) error
	// Enter enters the leader election
	Enter(EnterProposal) (*election.EnterResponse, error)
	// Withdraw withdraws a candidate from the leader election
	Withdraw(WithdrawProposal) (*election.WithdrawResponse, error)
	// Anoint anoints a candidate leader
	Anoint(AnointProposal) (*election.AnointResponse, error)
	// Promote promotes a candidate
	Promote(PromoteProposal) (*election.PromoteResponse, error)
	// Evict evicts a candidate from the election
	Evict(EvictProposal) (*election.EvictResponse, error)
	// GetTerm gets the current leadership term
	GetTerm(GetTermQuery) (*election.GetTermResponse, error)
	// Events listens for leadership events
	Events(EventsProposal)
}

type ServiceContext interface {
	Scheduler() rsm.Scheduler
	Sessions() Sessions
	Proposals() Proposals
}

func newServiceContext(service rsm.ServiceContext) ServiceContext {
	return &serviceContext{
		scheduler: service.Scheduler(),
		sessions:  newSessions(service.Sessions()),
		proposals: newProposals(service.Commands()),
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

type SnapshotWriter interface {
	WriteState(*LeaderElectionState) error
}

func newSnapshotWriter(writer io.Writer) SnapshotWriter {
	return &serviceSnapshotWriter{
		writer: writer,
	}
}

type serviceSnapshotWriter struct {
	writer io.Writer
}

func (w *serviceSnapshotWriter) WriteState(state *LeaderElectionState) error {
	bytes, err := proto.Marshal(state)
	if err != nil {
		return err
	}
	err = util.WriteBytes(w.writer, bytes)
	if err != nil {
		return err
	}
	return err
}

var _ SnapshotWriter = &serviceSnapshotWriter{}

type SnapshotReader interface {
	ReadState() (*LeaderElectionState, error)
}

func newSnapshotReader(reader io.Reader) SnapshotReader {
	return &serviceSnapshotReader{
		reader: reader,
	}
}

type serviceSnapshotReader struct {
	reader io.Reader
}

func (r *serviceSnapshotReader) ReadState() (*LeaderElectionState, error) {
	bytes, err := util.ReadBytes(r.reader)
	if err != nil {
		return nil, err
	}
	state := &LeaderElectionState{}
	err = proto.Unmarshal(bytes, state)
	if err != nil {
		return nil, err
	}
	return state, nil
}

var _ SnapshotReader = &serviceSnapshotReader{}

type Sessions interface {
	Get(SessionID) (Session, bool)
	List() []Session
}

func newSessions(sessions rsm.Sessions) Sessions {
	return &serviceSessions{
		sessions: sessions,
	}
}

type serviceSessions struct {
	sessions rsm.Sessions
}

func (s *serviceSessions) Get(id SessionID) (Session, bool) {
	session, ok := s.sessions.Get(rsm.SessionID(id))
	if !ok {
		return nil, false
	}
	return newSession(session), true
}

func (s *serviceSessions) List() []Session {
	serviceSessions := s.sessions.List()
	sessions := make([]Session, len(serviceSessions))
	for i, serviceSession := range serviceSessions {
		sessions[i] = newSession(serviceSession)
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

func newWatcher(watcher rsm.Watcher) Watcher {
	return &serviceWatcher{
		watcher: watcher,
	}
}

type serviceWatcher struct {
	watcher rsm.Watcher
}

func (s *serviceWatcher) Cancel() {
	s.watcher.Cancel()
}

var _ Watcher = &serviceWatcher{}

type Session interface {
	ID() SessionID
	State() SessionState
	Watch(func(SessionState)) Watcher
	Proposals() Proposals
}

func newSession(session rsm.Session) Session {
	return &serviceSession{
		session:   session,
		proposals: newProposals(session.Commands()),
	}
}

type serviceSession struct {
	session   rsm.Session
	proposals Proposals
}

func (s *serviceSession) ID() SessionID {
	return SessionID(s.session.ID())
}

func (s *serviceSession) Proposals() Proposals {
	return s.proposals
}

func (s *serviceSession) State() SessionState {
	return SessionState(s.session.State())
}

func (s *serviceSession) Watch(f func(SessionState)) Watcher {
	return newWatcher(s.session.Watch(func(state rsm.SessionState) {
		f(SessionState(state))
	}))
}

var _ Session = &serviceSession{}

type Proposals interface {
	Enter() EnterProposals
	Withdraw() WithdrawProposals
	Anoint() AnointProposals
	Promote() PromoteProposals
	Evict() EvictProposals
	Events() EventsProposals
}

func newProposals(commands rsm.Commands) Proposals {
	return &serviceProposals{
		enterProposals:    newEnterProposals(commands),
		withdrawProposals: newWithdrawProposals(commands),
		anointProposals:   newAnointProposals(commands),
		promoteProposals:  newPromoteProposals(commands),
		evictProposals:    newEvictProposals(commands),
		eventsProposals:   newEventsProposals(commands),
	}
}

type serviceProposals struct {
	enterProposals    EnterProposals
	withdrawProposals WithdrawProposals
	anointProposals   AnointProposals
	promoteProposals  PromoteProposals
	evictProposals    EvictProposals
	eventsProposals   EventsProposals
}

func (s *serviceProposals) Enter() EnterProposals {
	return s.enterProposals
}
func (s *serviceProposals) Withdraw() WithdrawProposals {
	return s.withdrawProposals
}
func (s *serviceProposals) Anoint() AnointProposals {
	return s.anointProposals
}
func (s *serviceProposals) Promote() PromoteProposals {
	return s.promoteProposals
}
func (s *serviceProposals) Evict() EvictProposals {
	return s.evictProposals
}
func (s *serviceProposals) Events() EventsProposals {
	return s.eventsProposals
}

var _ Proposals = &serviceProposals{}

type ProposalID uint64

type ProposalState int

const (
	ProposalComplete ProposalState = iota
	ProposalOpen
)

type Proposal interface {
	fmt.Stringer
	ID() ProposalID
	Session() Session
	State() ProposalState
	Watch(func(ProposalState)) Watcher
}

func newProposal(command rsm.Command) Proposal {
	return &serviceProposal{
		command: command,
	}
}

type serviceProposal struct {
	command rsm.Command
}

func (p *serviceProposal) ID() ProposalID {
	return ProposalID(p.command.ID())
}

func (p *serviceProposal) Session() Session {
	return newSession(p.command.Session())
}

func (p *serviceProposal) State() ProposalState {
	return ProposalState(p.command.State())
}

func (p *serviceProposal) Watch(f func(ProposalState)) Watcher {
	return newWatcher(p.command.Watch(func(state rsm.CommandState) {
		f(ProposalState(state))
	}))
}

func (p *serviceProposal) String() string {
	return fmt.Sprintf("ProposalID: %d, SessionID: %d", p.ID(), p.Session().ID())
}

var _ Proposal = &serviceProposal{}

type Query interface {
	fmt.Stringer
	Session() Session
}

func newQuery(query rsm.Query) Query {
	return &serviceQuery{
		query: query,
	}
}

type serviceQuery struct {
	query rsm.Query
}

func (p *serviceQuery) Session() Session {
	return newSession(p.query.Session())
}

func (p *serviceQuery) String() string {
	return fmt.Sprintf("SessionID: %d", p.Session().ID())
}

var _ Query = &serviceQuery{}

type EnterProposals interface {
	Get(ProposalID) (EnterProposal, bool)
	List() []EnterProposal
}

func newEnterProposals(commands rsm.Commands) EnterProposals {
	return &enterProposals{
		commands: commands,
	}
}

type enterProposals struct {
	commands rsm.Commands
}

func (p *enterProposals) Get(id ProposalID) (EnterProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	proposal, err := newEnterProposal(command)
	if err != nil {
		log.Error(err)
		return nil, false
	}
	return proposal, true
}

func (p *enterProposals) List() []EnterProposal {
	commands := p.commands.List(rsm.OperationID(1))
	proposals := make([]EnterProposal, len(commands))
	for i, command := range commands {
		proposal, err := newEnterProposal(command)
		if err != nil {
			log.Error(err)
		} else {
			proposals[i] = proposal
		}
	}
	return proposals
}

var _ EnterProposals = &enterProposals{}

type EnterProposal interface {
	Proposal
	Request() *election.EnterRequest
}

func newEnterProposal(command rsm.Command) (EnterProposal, error) {
	request := &election.EnterRequest{}
	if err := proto.Unmarshal(command.Input(), request); err != nil {
		return nil, err
	}
	return &enterProposal{
		Proposal: newProposal(command),
		command:  command,
		request:  request,
	}, nil
}

type enterProposal struct {
	Proposal
	command rsm.Command
	request *election.EnterRequest
}

func (p *enterProposal) Request() *election.EnterRequest {
	return p.request
}

func (p *enterProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ EnterProposal = &enterProposal{}

type WithdrawProposals interface {
	Get(ProposalID) (WithdrawProposal, bool)
	List() []WithdrawProposal
}

func newWithdrawProposals(commands rsm.Commands) WithdrawProposals {
	return &withdrawProposals{
		commands: commands,
	}
}

type withdrawProposals struct {
	commands rsm.Commands
}

func (p *withdrawProposals) Get(id ProposalID) (WithdrawProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	proposal, err := newWithdrawProposal(command)
	if err != nil {
		log.Error(err)
		return nil, false
	}
	return proposal, true
}

func (p *withdrawProposals) List() []WithdrawProposal {
	commands := p.commands.List(rsm.OperationID(2))
	proposals := make([]WithdrawProposal, len(commands))
	for i, command := range commands {
		proposal, err := newWithdrawProposal(command)
		if err != nil {
			log.Error(err)
		} else {
			proposals[i] = proposal
		}
	}
	return proposals
}

var _ WithdrawProposals = &withdrawProposals{}

type WithdrawProposal interface {
	Proposal
	Request() *election.WithdrawRequest
}

func newWithdrawProposal(command rsm.Command) (WithdrawProposal, error) {
	request := &election.WithdrawRequest{}
	if err := proto.Unmarshal(command.Input(), request); err != nil {
		return nil, err
	}
	return &withdrawProposal{
		Proposal: newProposal(command),
		command:  command,
		request:  request,
	}, nil
}

type withdrawProposal struct {
	Proposal
	command rsm.Command
	request *election.WithdrawRequest
}

func (p *withdrawProposal) Request() *election.WithdrawRequest {
	return p.request
}

func (p *withdrawProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ WithdrawProposal = &withdrawProposal{}

type AnointProposals interface {
	Get(ProposalID) (AnointProposal, bool)
	List() []AnointProposal
}

func newAnointProposals(commands rsm.Commands) AnointProposals {
	return &anointProposals{
		commands: commands,
	}
}

type anointProposals struct {
	commands rsm.Commands
}

func (p *anointProposals) Get(id ProposalID) (AnointProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	proposal, err := newAnointProposal(command)
	if err != nil {
		log.Error(err)
		return nil, false
	}
	return proposal, true
}

func (p *anointProposals) List() []AnointProposal {
	commands := p.commands.List(rsm.OperationID(3))
	proposals := make([]AnointProposal, len(commands))
	for i, command := range commands {
		proposal, err := newAnointProposal(command)
		if err != nil {
			log.Error(err)
		} else {
			proposals[i] = proposal
		}
	}
	return proposals
}

var _ AnointProposals = &anointProposals{}

type AnointProposal interface {
	Proposal
	Request() *election.AnointRequest
}

func newAnointProposal(command rsm.Command) (AnointProposal, error) {
	request := &election.AnointRequest{}
	if err := proto.Unmarshal(command.Input(), request); err != nil {
		return nil, err
	}
	return &anointProposal{
		Proposal: newProposal(command),
		command:  command,
		request:  request,
	}, nil
}

type anointProposal struct {
	Proposal
	command rsm.Command
	request *election.AnointRequest
}

func (p *anointProposal) Request() *election.AnointRequest {
	return p.request
}

func (p *anointProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ AnointProposal = &anointProposal{}

type PromoteProposals interface {
	Get(ProposalID) (PromoteProposal, bool)
	List() []PromoteProposal
}

func newPromoteProposals(commands rsm.Commands) PromoteProposals {
	return &promoteProposals{
		commands: commands,
	}
}

type promoteProposals struct {
	commands rsm.Commands
}

func (p *promoteProposals) Get(id ProposalID) (PromoteProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	proposal, err := newPromoteProposal(command)
	if err != nil {
		log.Error(err)
		return nil, false
	}
	return proposal, true
}

func (p *promoteProposals) List() []PromoteProposal {
	commands := p.commands.List(rsm.OperationID(4))
	proposals := make([]PromoteProposal, len(commands))
	for i, command := range commands {
		proposal, err := newPromoteProposal(command)
		if err != nil {
			log.Error(err)
		} else {
			proposals[i] = proposal
		}
	}
	return proposals
}

var _ PromoteProposals = &promoteProposals{}

type PromoteProposal interface {
	Proposal
	Request() *election.PromoteRequest
}

func newPromoteProposal(command rsm.Command) (PromoteProposal, error) {
	request := &election.PromoteRequest{}
	if err := proto.Unmarshal(command.Input(), request); err != nil {
		return nil, err
	}
	return &promoteProposal{
		Proposal: newProposal(command),
		command:  command,
		request:  request,
	}, nil
}

type promoteProposal struct {
	Proposal
	command rsm.Command
	request *election.PromoteRequest
}

func (p *promoteProposal) Request() *election.PromoteRequest {
	return p.request
}

func (p *promoteProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ PromoteProposal = &promoteProposal{}

type EvictProposals interface {
	Get(ProposalID) (EvictProposal, bool)
	List() []EvictProposal
}

func newEvictProposals(commands rsm.Commands) EvictProposals {
	return &evictProposals{
		commands: commands,
	}
}

type evictProposals struct {
	commands rsm.Commands
}

func (p *evictProposals) Get(id ProposalID) (EvictProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	proposal, err := newEvictProposal(command)
	if err != nil {
		log.Error(err)
		return nil, false
	}
	return proposal, true
}

func (p *evictProposals) List() []EvictProposal {
	commands := p.commands.List(rsm.OperationID(5))
	proposals := make([]EvictProposal, len(commands))
	for i, command := range commands {
		proposal, err := newEvictProposal(command)
		if err != nil {
			log.Error(err)
		} else {
			proposals[i] = proposal
		}
	}
	return proposals
}

var _ EvictProposals = &evictProposals{}

type EvictProposal interface {
	Proposal
	Request() *election.EvictRequest
}

func newEvictProposal(command rsm.Command) (EvictProposal, error) {
	request := &election.EvictRequest{}
	if err := proto.Unmarshal(command.Input(), request); err != nil {
		return nil, err
	}
	return &evictProposal{
		Proposal: newProposal(command),
		command:  command,
		request:  request,
	}, nil
}

type evictProposal struct {
	Proposal
	command rsm.Command
	request *election.EvictRequest
}

func (p *evictProposal) Request() *election.EvictRequest {
	return p.request
}

func (p *evictProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ EvictProposal = &evictProposal{}

type GetTermQuery interface {
	Query
	Request() *election.GetTermRequest
}

func newGetTermQuery(query rsm.Query) (GetTermQuery, error) {
	request := &election.GetTermRequest{}
	if err := proto.Unmarshal(query.Input(), request); err != nil {
		return nil, err
	}
	return &getTermQuery{
		Query:   newQuery(query),
		query:   query,
		request: request,
	}, nil
}

type getTermQuery struct {
	Query
	query   rsm.Query
	request *election.GetTermRequest
}

func (p *getTermQuery) Request() *election.GetTermRequest {
	return p.request
}

func (p *getTermQuery) String() string {
	return fmt.Sprintf("SessionID=%d", p.Session().ID())
}

var _ GetTermQuery = &getTermQuery{}

type EventsProposals interface {
	Get(ProposalID) (EventsProposal, bool)
	List() []EventsProposal
}

func newEventsProposals(commands rsm.Commands) EventsProposals {
	return &eventsProposals{
		commands: commands,
	}
}

type eventsProposals struct {
	commands rsm.Commands
}

func (p *eventsProposals) Get(id ProposalID) (EventsProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	proposal, err := newEventsProposal(command)
	if err != nil {
		log.Error(err)
		return nil, false
	}
	return proposal, true
}

func (p *eventsProposals) List() []EventsProposal {
	commands := p.commands.List(rsm.OperationID(7))
	proposals := make([]EventsProposal, len(commands))
	for i, command := range commands {
		proposal, err := newEventsProposal(command)
		if err != nil {
			log.Error(err)
		} else {
			proposals[i] = proposal
		}
	}
	return proposals
}

var _ EventsProposals = &eventsProposals{}

type EventsProposal interface {
	Proposal
	Request() *election.EventsRequest
	Notify(*election.EventsResponse)
	Close()
}

func newEventsProposal(command rsm.Command) (EventsProposal, error) {
	request := &election.EventsRequest{}
	if err := proto.Unmarshal(command.Input(), request); err != nil {
		return nil, err
	}
	return &eventsProposal{
		Proposal: newProposal(command),
		command:  command,
		request:  request,
	}, nil
}

type eventsProposal struct {
	Proposal
	command rsm.Command
	request *election.EventsRequest
	closed  bool
}

func (p *eventsProposal) Request() *election.EventsRequest {
	return p.request
}

func (p *eventsProposal) Notify(response *election.EventsResponse) {
	if p.closed {
		return
	}
	log.Debugf("Notifying EventsProposal %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		err = errors.NewInternal(err.Error())
		log.Errorf("Notifying EventsProposal %s failed: %v", p, err)
		p.command.Output(nil, err)
		p.command.Close()
		p.closed = true
	} else {
		p.command.Output(output, nil)
	}
}

func (p *eventsProposal) Close() {
	p.command.Close()
	p.closed = true
}

func (p *eventsProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ EventsProposal = &eventsProposal{}
