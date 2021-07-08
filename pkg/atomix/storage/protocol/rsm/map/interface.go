// Code generated by atomix-go-framework. DO NOT EDIT.
package _map

import (
	"fmt"
	_map "github.com/atomix/atomix-api/go/atomix/primitive/map"
	rsm "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	util "github.com/atomix/atomix-go-framework/pkg/atomix/util"
	proto "github.com/golang/protobuf/proto"
	"io"
)

type Service interface {
	ServiceContext
	Backup(SnapshotWriter) error
	Restore(SnapshotReader) error
	// Size returns the size of the map
	Size(SizeQuery) error
	// Put puts an entry into the map
	Put(PutProposal) error
	// Get gets the entry for a key
	Get(GetQuery) error
	// Remove removes an entry from the map
	Remove(RemoveProposal) error
	// Clear removes all entries from the map
	Clear(ClearProposal) error
	// Events listens for change events
	Events(EventsProposal) error
	// Entries lists all entries in the map
	Entries(EntriesQuery) error
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

func newWatcher(watcher rsm.SessionStateWatcher) Watcher {
	return &serviceWatcher{
		watcher: watcher,
	}
}

type serviceWatcher struct {
	watcher rsm.SessionStateWatcher
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

type Proposal interface {
	fmt.Stringer
	ID() ProposalID
	Session() Session
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
	Request() (*_map.SizeRequest, error)
	Reply(*_map.SizeResponse) error
}

func newSizeQuery(query rsm.Query) SizeQuery {
	return &sizeQuery{
		Query: newQuery(query),
		query: query,
	}
}

type sizeQuery struct {
	Query
	query rsm.Query
}

func (p *sizeQuery) Request() (*_map.SizeRequest, error) {
	request := &_map.SizeRequest{}
	if err := proto.Unmarshal(p.query.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received SizeQuery %s: %s", p, request)
	return request, nil
}

func (p *sizeQuery) Reply(response *_map.SizeResponse) error {
	log.Debugf("Sending SizeQuery %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.query.Output(output, nil)
	p.query.Close()
	return nil
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
	return newPutProposal(command), true
}

func (p *putProposals) List() []PutProposal {
	commands := p.commands.List(rsm.OperationID(2))
	proposals := make([]PutProposal, len(commands))
	for i, command := range commands {
		proposals[i] = newPutProposal(command)
	}
	return proposals
}

var _ PutProposals = &putProposals{}

type PutProposal interface {
	Proposal
	Request() (*_map.PutRequest, error)
	Reply(*_map.PutResponse) error
}

func newPutProposal(command rsm.Command) PutProposal {
	return &putProposal{
		Proposal: newProposal(command),
		command:  command,
	}
}

type putProposal struct {
	Proposal
	command rsm.Command
}

func (p *putProposal) Request() (*_map.PutRequest, error) {
	request := &_map.PutRequest{}
	if err := proto.Unmarshal(p.command.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received PutProposal %s: %s", p, request)
	return request, nil
}

func (p *putProposal) Reply(response *_map.PutResponse) error {
	log.Debugf("Sending PutProposal %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.command.Output(output, nil)
	p.command.Close()
	return nil
}

func (p *putProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ PutProposal = &putProposal{}

type GetQuery interface {
	Query
	Request() (*_map.GetRequest, error)
	Reply(*_map.GetResponse) error
}

func newGetQuery(query rsm.Query) GetQuery {
	return &getQuery{
		Query: newQuery(query),
		query: query,
	}
}

type getQuery struct {
	Query
	query rsm.Query
}

func (p *getQuery) Request() (*_map.GetRequest, error) {
	request := &_map.GetRequest{}
	if err := proto.Unmarshal(p.query.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received GetQuery %s: %s", p, request)
	return request, nil
}

func (p *getQuery) Reply(response *_map.GetResponse) error {
	log.Debugf("Sending GetQuery %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.query.Output(output, nil)
	p.query.Close()
	return nil
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
	return newRemoveProposal(command), true
}

func (p *removeProposals) List() []RemoveProposal {
	commands := p.commands.List(rsm.OperationID(4))
	proposals := make([]RemoveProposal, len(commands))
	for i, command := range commands {
		proposals[i] = newRemoveProposal(command)
	}
	return proposals
}

var _ RemoveProposals = &removeProposals{}

type RemoveProposal interface {
	Proposal
	Request() (*_map.RemoveRequest, error)
	Reply(*_map.RemoveResponse) error
}

func newRemoveProposal(command rsm.Command) RemoveProposal {
	return &removeProposal{
		Proposal: newProposal(command),
		command:  command,
	}
}

type removeProposal struct {
	Proposal
	command rsm.Command
}

func (p *removeProposal) Request() (*_map.RemoveRequest, error) {
	request := &_map.RemoveRequest{}
	if err := proto.Unmarshal(p.command.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received RemoveProposal %s: %s", p, request)
	return request, nil
}

func (p *removeProposal) Reply(response *_map.RemoveResponse) error {
	log.Debugf("Sending RemoveProposal %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.command.Output(output, nil)
	p.command.Close()
	return nil
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
	return newClearProposal(command), true
}

func (p *clearProposals) List() []ClearProposal {
	commands := p.commands.List(rsm.OperationID(5))
	proposals := make([]ClearProposal, len(commands))
	for i, command := range commands {
		proposals[i] = newClearProposal(command)
	}
	return proposals
}

var _ ClearProposals = &clearProposals{}

type ClearProposal interface {
	Proposal
	Request() (*_map.ClearRequest, error)
	Reply(*_map.ClearResponse) error
}

func newClearProposal(command rsm.Command) ClearProposal {
	return &clearProposal{
		Proposal: newProposal(command),
		command:  command,
	}
}

type clearProposal struct {
	Proposal
	command rsm.Command
}

func (p *clearProposal) Request() (*_map.ClearRequest, error) {
	request := &_map.ClearRequest{}
	if err := proto.Unmarshal(p.command.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received ClearProposal %s: %s", p, request)
	return request, nil
}

func (p *clearProposal) Reply(response *_map.ClearResponse) error {
	log.Debugf("Sending ClearProposal %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.command.Output(output, nil)
	p.command.Close()
	return nil
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
	return newEventsProposal(command), true
}

func (p *eventsProposals) List() []EventsProposal {
	commands := p.commands.List(rsm.OperationID(6))
	proposals := make([]EventsProposal, len(commands))
	for i, command := range commands {
		proposals[i] = newEventsProposal(command)
	}
	return proposals
}

var _ EventsProposals = &eventsProposals{}

type EventsProposal interface {
	Proposal
	Request() (*_map.EventsRequest, error)
	Notify(*_map.EventsResponse) error
	Close() error
}

func newEventsProposal(command rsm.Command) EventsProposal {
	return &eventsProposal{
		Proposal: newProposal(command),
		command:  command,
	}
}

type eventsProposal struct {
	Proposal
	command rsm.Command
}

func (p *eventsProposal) Request() (*_map.EventsRequest, error) {
	request := &_map.EventsRequest{}
	if err := proto.Unmarshal(p.command.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received EventsProposal %s: %s", p, request)
	return request, nil
}

func (p *eventsProposal) Notify(response *_map.EventsResponse) error {
	log.Debugf("Notifying EventsProposal %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.command.Output(output, nil)
	return nil
}

func (p *eventsProposal) Close() error {
	p.command.Close()
	return nil
}

func (p *eventsProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ EventsProposal = &eventsProposal{}

type EntriesQuery interface {
	Query
	Request() (*_map.EntriesRequest, error)
	Notify(*_map.EntriesResponse) error
	Close() error
}

func newEntriesQuery(query rsm.Query) EntriesQuery {
	return &entriesQuery{
		Query: newQuery(query),
		query: query,
	}
}

type entriesQuery struct {
	Query
	query rsm.Query
}

func (p *entriesQuery) Request() (*_map.EntriesRequest, error) {
	request := &_map.EntriesRequest{}
	if err := proto.Unmarshal(p.query.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received EntriesQuery %s: %s", p, request)
	return request, nil
}

func (p *entriesQuery) Notify(response *_map.EntriesResponse) error {
	log.Debugf("Notifying EntriesQuery %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.query.Output(output, nil)
	return nil
}

func (p *entriesQuery) Close() error {
	p.query.Close()
	return nil
}

func (p *entriesQuery) String() string {
	return fmt.Sprintf("SessionID=%d", p.Session().ID())
}

var _ EntriesQuery = &entriesQuery{}
