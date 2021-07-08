// Code generated by atomix-go-framework. DO NOT EDIT.
package list

import (
	"fmt"
	list "github.com/atomix/atomix-api/go/atomix/primitive/list"
	rsm "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	util "github.com/atomix/atomix-go-framework/pkg/atomix/util"
	proto "github.com/golang/protobuf/proto"
	"io"
)

type Service interface {
	ServiceContext
	Backup(SnapshotWriter) error
	Restore(SnapshotReader) error
	// Size gets the number of elements in the list
	Size(SizeQuery) error
	// Append appends a value to the list
	Append(AppendProposal) error
	// Insert inserts a value at a specific index in the list
	Insert(InsertProposal) error
	// Get gets the value at an index in the list
	Get(GetQuery) error
	// Set sets the value at an index in the list
	Set(SetProposal) error
	// Remove removes an element from the list
	Remove(RemoveProposal) error
	// Clear removes all elements from the list
	Clear(ClearProposal) error
	// Events listens for change events
	Events(EventsProposal) error
	// Elements streams all elements in the list
	Elements(ElementsQuery) error
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
	WriteState(*ListState) error
}

func newSnapshotWriter(writer io.Writer) SnapshotWriter {
	return &serviceSnapshotWriter{
		writer: writer,
	}
}

type serviceSnapshotWriter struct {
	writer io.Writer
}

func (w *serviceSnapshotWriter) WriteState(state *ListState) error {
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
	ReadState() (*ListState, error)
}

func newSnapshotReader(reader io.Reader) SnapshotReader {
	return &serviceSnapshotReader{
		reader: reader,
	}
}

type serviceSnapshotReader struct {
	reader io.Reader
}

func (r *serviceSnapshotReader) ReadState() (*ListState, error) {
	bytes, err := util.ReadBytes(r.reader)
	if err != nil {
		return nil, err
	}
	state := &ListState{}
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
	Append() AppendProposals
	Insert() InsertProposals
	Set() SetProposals
	Remove() RemoveProposals
	Clear() ClearProposals
	Events() EventsProposals
}

func newProposals(commands rsm.Commands) Proposals {
	return &serviceProposals{
		appendProposals: newAppendProposals(commands),
		insertProposals: newInsertProposals(commands),
		setProposals:    newSetProposals(commands),
		removeProposals: newRemoveProposals(commands),
		clearProposals:  newClearProposals(commands),
		eventsProposals: newEventsProposals(commands),
	}
}

type serviceProposals struct {
	appendProposals AppendProposals
	insertProposals InsertProposals
	setProposals    SetProposals
	removeProposals RemoveProposals
	clearProposals  ClearProposals
	eventsProposals EventsProposals
}

func (s *serviceProposals) Append() AppendProposals {
	return s.appendProposals
}
func (s *serviceProposals) Insert() InsertProposals {
	return s.insertProposals
}
func (s *serviceProposals) Set() SetProposals {
	return s.setProposals
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
	Request() (*list.SizeRequest, error)
	Reply(*list.SizeResponse) error
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

func (p *sizeQuery) Request() (*list.SizeRequest, error) {
	request := &list.SizeRequest{}
	if err := proto.Unmarshal(p.query.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received SizeQuery %s: %s", p, request)
	return request, nil
}

func (p *sizeQuery) Reply(response *list.SizeResponse) error {
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

type AppendProposals interface {
	Get(ProposalID) (AppendProposal, bool)
	List() []AppendProposal
}

func newAppendProposals(commands rsm.Commands) AppendProposals {
	return &appendProposals{
		commands: commands,
	}
}

type appendProposals struct {
	commands rsm.Commands
}

func (p *appendProposals) Get(id ProposalID) (AppendProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	return newAppendProposal(command), true
}

func (p *appendProposals) List() []AppendProposal {
	commands := p.commands.List(rsm.OperationID(2))
	proposals := make([]AppendProposal, len(commands))
	for i, command := range commands {
		proposals[i] = newAppendProposal(command)
	}
	return proposals
}

var _ AppendProposals = &appendProposals{}

type AppendProposal interface {
	Proposal
	Request() (*list.AppendRequest, error)
	Reply(*list.AppendResponse) error
}

func newAppendProposal(command rsm.Command) AppendProposal {
	return &appendProposal{
		Proposal: newProposal(command),
		command:  command,
	}
}

type appendProposal struct {
	Proposal
	command rsm.Command
}

func (p *appendProposal) Request() (*list.AppendRequest, error) {
	request := &list.AppendRequest{}
	if err := proto.Unmarshal(p.command.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received AppendProposal %s: %s", p, request)
	return request, nil
}

func (p *appendProposal) Reply(response *list.AppendResponse) error {
	log.Debugf("Sending AppendProposal %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.command.Output(output, nil)
	p.command.Close()
	return nil
}

func (p *appendProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ AppendProposal = &appendProposal{}

type InsertProposals interface {
	Get(ProposalID) (InsertProposal, bool)
	List() []InsertProposal
}

func newInsertProposals(commands rsm.Commands) InsertProposals {
	return &insertProposals{
		commands: commands,
	}
}

type insertProposals struct {
	commands rsm.Commands
}

func (p *insertProposals) Get(id ProposalID) (InsertProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	return newInsertProposal(command), true
}

func (p *insertProposals) List() []InsertProposal {
	commands := p.commands.List(rsm.OperationID(3))
	proposals := make([]InsertProposal, len(commands))
	for i, command := range commands {
		proposals[i] = newInsertProposal(command)
	}
	return proposals
}

var _ InsertProposals = &insertProposals{}

type InsertProposal interface {
	Proposal
	Request() (*list.InsertRequest, error)
	Reply(*list.InsertResponse) error
}

func newInsertProposal(command rsm.Command) InsertProposal {
	return &insertProposal{
		Proposal: newProposal(command),
		command:  command,
	}
}

type insertProposal struct {
	Proposal
	command rsm.Command
}

func (p *insertProposal) Request() (*list.InsertRequest, error) {
	request := &list.InsertRequest{}
	if err := proto.Unmarshal(p.command.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received InsertProposal %s: %s", p, request)
	return request, nil
}

func (p *insertProposal) Reply(response *list.InsertResponse) error {
	log.Debugf("Sending InsertProposal %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.command.Output(output, nil)
	p.command.Close()
	return nil
}

func (p *insertProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ InsertProposal = &insertProposal{}

type GetQuery interface {
	Query
	Request() (*list.GetRequest, error)
	Reply(*list.GetResponse) error
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

func (p *getQuery) Request() (*list.GetRequest, error) {
	request := &list.GetRequest{}
	if err := proto.Unmarshal(p.query.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received GetQuery %s: %s", p, request)
	return request, nil
}

func (p *getQuery) Reply(response *list.GetResponse) error {
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

type SetProposals interface {
	Get(ProposalID) (SetProposal, bool)
	List() []SetProposal
}

func newSetProposals(commands rsm.Commands) SetProposals {
	return &setProposals{
		commands: commands,
	}
}

type setProposals struct {
	commands rsm.Commands
}

func (p *setProposals) Get(id ProposalID) (SetProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	return newSetProposal(command), true
}

func (p *setProposals) List() []SetProposal {
	commands := p.commands.List(rsm.OperationID(5))
	proposals := make([]SetProposal, len(commands))
	for i, command := range commands {
		proposals[i] = newSetProposal(command)
	}
	return proposals
}

var _ SetProposals = &setProposals{}

type SetProposal interface {
	Proposal
	Request() (*list.SetRequest, error)
	Reply(*list.SetResponse) error
}

func newSetProposal(command rsm.Command) SetProposal {
	return &setProposal{
		Proposal: newProposal(command),
		command:  command,
	}
}

type setProposal struct {
	Proposal
	command rsm.Command
}

func (p *setProposal) Request() (*list.SetRequest, error) {
	request := &list.SetRequest{}
	if err := proto.Unmarshal(p.command.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received SetProposal %s: %s", p, request)
	return request, nil
}

func (p *setProposal) Reply(response *list.SetResponse) error {
	log.Debugf("Sending SetProposal %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.command.Output(output, nil)
	p.command.Close()
	return nil
}

func (p *setProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ SetProposal = &setProposal{}

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
	commands := p.commands.List(rsm.OperationID(6))
	proposals := make([]RemoveProposal, len(commands))
	for i, command := range commands {
		proposals[i] = newRemoveProposal(command)
	}
	return proposals
}

var _ RemoveProposals = &removeProposals{}

type RemoveProposal interface {
	Proposal
	Request() (*list.RemoveRequest, error)
	Reply(*list.RemoveResponse) error
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

func (p *removeProposal) Request() (*list.RemoveRequest, error) {
	request := &list.RemoveRequest{}
	if err := proto.Unmarshal(p.command.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received RemoveProposal %s: %s", p, request)
	return request, nil
}

func (p *removeProposal) Reply(response *list.RemoveResponse) error {
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
	commands := p.commands.List(rsm.OperationID(7))
	proposals := make([]ClearProposal, len(commands))
	for i, command := range commands {
		proposals[i] = newClearProposal(command)
	}
	return proposals
}

var _ ClearProposals = &clearProposals{}

type ClearProposal interface {
	Proposal
	Request() (*list.ClearRequest, error)
	Reply(*list.ClearResponse) error
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

func (p *clearProposal) Request() (*list.ClearRequest, error) {
	request := &list.ClearRequest{}
	if err := proto.Unmarshal(p.command.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received ClearProposal %s: %s", p, request)
	return request, nil
}

func (p *clearProposal) Reply(response *list.ClearResponse) error {
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
	commands := p.commands.List(rsm.OperationID(8))
	proposals := make([]EventsProposal, len(commands))
	for i, command := range commands {
		proposals[i] = newEventsProposal(command)
	}
	return proposals
}

var _ EventsProposals = &eventsProposals{}

type EventsProposal interface {
	Proposal
	Request() (*list.EventsRequest, error)
	Notify(*list.EventsResponse) error
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

func (p *eventsProposal) Request() (*list.EventsRequest, error) {
	request := &list.EventsRequest{}
	if err := proto.Unmarshal(p.command.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received EventsProposal %s: %s", p, request)
	return request, nil
}

func (p *eventsProposal) Notify(response *list.EventsResponse) error {
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

type ElementsQuery interface {
	Query
	Request() (*list.ElementsRequest, error)
	Notify(*list.ElementsResponse) error
	Close() error
}

func newElementsQuery(query rsm.Query) ElementsQuery {
	return &elementsQuery{
		Query: newQuery(query),
		query: query,
	}
}

type elementsQuery struct {
	Query
	query rsm.Query
}

func (p *elementsQuery) Request() (*list.ElementsRequest, error) {
	request := &list.ElementsRequest{}
	if err := proto.Unmarshal(p.query.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received ElementsQuery %s: %s", p, request)
	return request, nil
}

func (p *elementsQuery) Notify(response *list.ElementsResponse) error {
	log.Debugf("Notifying ElementsQuery %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.query.Output(output, nil)
	return nil
}

func (p *elementsQuery) Close() error {
	p.query.Close()
	return nil
}

func (p *elementsQuery) String() string {
	return fmt.Sprintf("SessionID=%d", p.Session().ID())
}

var _ ElementsQuery = &elementsQuery{}
