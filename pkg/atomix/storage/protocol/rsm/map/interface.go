// Code generated by atomix-go-framework. DO NOT EDIT.
package _map

import (
	"fmt"
	errors "github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	rsm "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	util "github.com/atomix/atomix-go-framework/pkg/atomix/util"
	proto "github.com/golang/protobuf/proto"
	"io"

	_map "github.com/atomix/atomix-api/go/atomix/primitive/map"
)

type Service interface {
	ServiceContext
	Backup(SnapshotWriter) error
	Restore(SnapshotReader) error
	// Size returns the size of the map
	Size(SizeQuery) (*_map.SizeResponse, error)
	// Put puts an entry into the map
	Put(PutProposal) (*_map.PutResponse, error)
	// Get gets the entry for a key
	Get(GetQuery) (*_map.GetResponse, error)
	// Remove removes an entry from the map
	Remove(RemoveProposal) (*_map.RemoveResponse, error)
	// Clear removes all entries from the map
	Clear(ClearProposal) (*_map.ClearResponse, error)
	// Events listens for change events
	Events(EventsProposal)
	// Entries lists all entries in the map
	Entries(EntriesQuery)
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
	WriteState(*MapState) error
}

func newSnapshotWriter(writer io.Writer) SnapshotWriter {
	return &serviceSnapshotWriter{
		writer: writer,
	}
}

type serviceSnapshotWriter struct {
	writer io.Writer
}

func (w *serviceSnapshotWriter) WriteState(state *MapState) error {
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
	ReadState() (*MapState, error)
}

func newSnapshotReader(reader io.Reader) SnapshotReader {
	return &serviceSnapshotReader{
		reader: reader,
	}
}

type serviceSnapshotReader struct {
	reader io.Reader
}

func (r *serviceSnapshotReader) ReadState() (*MapState, error) {
	bytes, err := util.ReadBytes(r.reader)
	if err != nil {
		return nil, err
	}
	state := &MapState{}
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
	Put() PutProposals
	Remove() RemoveProposals
	Clear() ClearProposals
	Events() EventsProposals
}

func newProposals(commands rsm.Commands) Proposals {
	return &serviceProposals{
		putProposals:    newPutProposals(commands),
		removeProposals: newRemoveProposals(commands),
		clearProposals:  newClearProposals(commands),
		eventsProposals: newEventsProposals(commands),
	}
}

type serviceProposals struct {
	putProposals    PutProposals
	removeProposals RemoveProposals
	clearProposals  ClearProposals
	eventsProposals EventsProposals
}

func (s *serviceProposals) Put() PutProposals {
	return s.putProposals
}
func (s *serviceProposals) Remove() RemoveProposals {
	return s.removeProposals
}
func (s *serviceProposals) Clear() ClearProposals {
	return s.clearProposals
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

type SizeQuery interface {
	Query
	Request() *_map.SizeRequest
}

func newSizeQuery(query rsm.Query) (SizeQuery, error) {
	request := &_map.SizeRequest{}
	if err := proto.Unmarshal(query.Input(), request); err != nil {
		return nil, err
	}
	return &sizeQuery{
		Query:   newQuery(query),
		query:   query,
		request: request,
	}, nil
}

type sizeQuery struct {
	Query
	query   rsm.Query
	request *_map.SizeRequest
}

func (p *sizeQuery) Request() *_map.SizeRequest {
	return p.request
}

func (p *sizeQuery) String() string {
	return fmt.Sprintf("SessionID=%d", p.Session().ID())
}

var _ SizeQuery = &sizeQuery{}

type PutProposals interface {
	Get(ProposalID) (PutProposal, bool)
	List() []PutProposal
}

func newPutProposals(commands rsm.Commands) PutProposals {
	return &putProposals{
		commands: commands,
	}
}

type putProposals struct {
	commands rsm.Commands
}

func (p *putProposals) Get(id ProposalID) (PutProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	proposal, err := newPutProposal(command)
	if err != nil {
		log.Error(err)
		return nil, false
	}
	return proposal, true
}

func (p *putProposals) List() []PutProposal {
	commands := p.commands.List(rsm.OperationID(2))
	proposals := make([]PutProposal, len(commands))
	for i, command := range commands {
		proposal, err := newPutProposal(command)
		if err != nil {
			log.Error(err)
		} else {
			proposals[i] = proposal
		}
	}
	return proposals
}

var _ PutProposals = &putProposals{}

type PutProposal interface {
	Proposal
	Request() *_map.PutRequest
}

func newPutProposal(command rsm.Command) (PutProposal, error) {
	request := &_map.PutRequest{}
	if err := proto.Unmarshal(command.Input(), request); err != nil {
		return nil, err
	}
	return &putProposal{
		Proposal: newProposal(command),
		command:  command,
		request:  request,
	}, nil
}

type putProposal struct {
	Proposal
	command rsm.Command
	request *_map.PutRequest
}

func (p *putProposal) Request() *_map.PutRequest {
	return p.request
}

func (p *putProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ PutProposal = &putProposal{}

type GetQuery interface {
	Query
	Request() *_map.GetRequest
}

func newGetQuery(query rsm.Query) (GetQuery, error) {
	request := &_map.GetRequest{}
	if err := proto.Unmarshal(query.Input(), request); err != nil {
		return nil, err
	}
	return &getQuery{
		Query:   newQuery(query),
		query:   query,
		request: request,
	}, nil
}

type getQuery struct {
	Query
	query   rsm.Query
	request *_map.GetRequest
}

func (p *getQuery) Request() *_map.GetRequest {
	return p.request
}

func (p *getQuery) String() string {
	return fmt.Sprintf("SessionID=%d", p.Session().ID())
}

var _ GetQuery = &getQuery{}

type RemoveProposals interface {
	Get(ProposalID) (RemoveProposal, bool)
	List() []RemoveProposal
}

func newRemoveProposals(commands rsm.Commands) RemoveProposals {
	return &removeProposals{
		commands: commands,
	}
}

type removeProposals struct {
	commands rsm.Commands
}

func (p *removeProposals) Get(id ProposalID) (RemoveProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	proposal, err := newRemoveProposal(command)
	if err != nil {
		log.Error(err)
		return nil, false
	}
	return proposal, true
}

func (p *removeProposals) List() []RemoveProposal {
	commands := p.commands.List(rsm.OperationID(4))
	proposals := make([]RemoveProposal, len(commands))
	for i, command := range commands {
		proposal, err := newRemoveProposal(command)
		if err != nil {
			log.Error(err)
		} else {
			proposals[i] = proposal
		}
	}
	return proposals
}

var _ RemoveProposals = &removeProposals{}

type RemoveProposal interface {
	Proposal
	Request() *_map.RemoveRequest
}

func newRemoveProposal(command rsm.Command) (RemoveProposal, error) {
	request := &_map.RemoveRequest{}
	if err := proto.Unmarshal(command.Input(), request); err != nil {
		return nil, err
	}
	return &removeProposal{
		Proposal: newProposal(command),
		command:  command,
		request:  request,
	}, nil
}

type removeProposal struct {
	Proposal
	command rsm.Command
	request *_map.RemoveRequest
}

func (p *removeProposal) Request() *_map.RemoveRequest {
	return p.request
}

func (p *removeProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ RemoveProposal = &removeProposal{}

type ClearProposals interface {
	Get(ProposalID) (ClearProposal, bool)
	List() []ClearProposal
}

func newClearProposals(commands rsm.Commands) ClearProposals {
	return &clearProposals{
		commands: commands,
	}
}

type clearProposals struct {
	commands rsm.Commands
}

func (p *clearProposals) Get(id ProposalID) (ClearProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	proposal, err := newClearProposal(command)
	if err != nil {
		log.Error(err)
		return nil, false
	}
	return proposal, true
}

func (p *clearProposals) List() []ClearProposal {
	commands := p.commands.List(rsm.OperationID(5))
	proposals := make([]ClearProposal, len(commands))
	for i, command := range commands {
		proposal, err := newClearProposal(command)
		if err != nil {
			log.Error(err)
		} else {
			proposals[i] = proposal
		}
	}
	return proposals
}

var _ ClearProposals = &clearProposals{}

type ClearProposal interface {
	Proposal
	Request() *_map.ClearRequest
}

func newClearProposal(command rsm.Command) (ClearProposal, error) {
	request := &_map.ClearRequest{}
	if err := proto.Unmarshal(command.Input(), request); err != nil {
		return nil, err
	}
	return &clearProposal{
		Proposal: newProposal(command),
		command:  command,
		request:  request,
	}, nil
}

type clearProposal struct {
	Proposal
	command rsm.Command
	request *_map.ClearRequest
}

func (p *clearProposal) Request() *_map.ClearRequest {
	return p.request
}

func (p *clearProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ ClearProposal = &clearProposal{}

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
	commands := p.commands.List(rsm.OperationID(6))
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
	Request() *_map.EventsRequest
	Notify(*_map.EventsResponse)
	Close()
}

func newEventsProposal(command rsm.Command) (EventsProposal, error) {
	request := &_map.EventsRequest{}
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
	request *_map.EventsRequest
	closed  bool
}

func (p *eventsProposal) Request() *_map.EventsRequest {
	return p.request
}

func (p *eventsProposal) Notify(response *_map.EventsResponse) {
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

type EntriesQuery interface {
	Query
	Request() *_map.EntriesRequest
	Notify(*_map.EntriesResponse)
	Close()
}

func newEntriesQuery(query rsm.Query) (EntriesQuery, error) {
	request := &_map.EntriesRequest{}
	if err := proto.Unmarshal(query.Input(), request); err != nil {
		return nil, err
	}
	return &entriesQuery{
		Query:   newQuery(query),
		query:   query,
		request: request,
	}, nil
}

type entriesQuery struct {
	Query
	query   rsm.Query
	request *_map.EntriesRequest
	closed  bool
}

func (p *entriesQuery) Request() *_map.EntriesRequest {
	return p.request
}

func (p *entriesQuery) Notify(response *_map.EntriesResponse) {
	if p.closed {
		return
	}
	log.Debugf("Notifying EntriesQuery %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		err = errors.NewInternal(err.Error())
		log.Errorf("Notifying EntriesQuery %s failed: %v", p, err)
		p.query.Output(nil, err)
		p.query.Close()
		p.closed = true
	} else {
		p.query.Output(output, nil)
	}
}

func (p *entriesQuery) Close() {
	p.query.Close()
	p.closed = true
}

func (p *entriesQuery) String() string {
	return fmt.Sprintf("SessionID=%d", p.Session().ID())
}

var _ EntriesQuery = &entriesQuery{}
