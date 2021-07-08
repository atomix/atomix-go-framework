// Code generated by atomix-go-framework. DO NOT EDIT.
package lock

import (
	"fmt"
	lock "github.com/atomix/atomix-api/go/atomix/primitive/lock"
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
	// Lock attempts to acquire the lock
	Lock(LockProposal) error
	// Unlock releases the lock
	Unlock(UnlockProposal) error
	// GetLock gets the lock state
	GetLock(GetLockQuery) error
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
	WriteState(*LockState) error
}

func newSnapshotWriter(writer io.Writer) SnapshotWriter {
	return &serviceSnapshotWriter{
		writer: writer,
	}
}

type serviceSnapshotWriter struct {
	writer io.Writer
}

func (w *serviceSnapshotWriter) WriteState(state *LockState) error {
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
	ReadState() (*LockState, error)
}

func newSnapshotReader(reader io.Reader) SnapshotReader {
	return &serviceSnapshotReader{
		reader: reader,
	}
}

type serviceSnapshotReader struct {
	reader io.Reader
}

func (r *serviceSnapshotReader) ReadState() (*LockState, error) {
	bytes, err := util.ReadBytes(r.reader)
	if err != nil {
		return nil, err
	}
	state := &LockState{}
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
	Lock() LockProposals
	Unlock() UnlockProposals
}

func newProposals(commands rsm.Commands) Proposals {
	return &serviceProposals{
		lockProposals:   newLockProposals(commands),
		unlockProposals: newUnlockProposals(commands),
	}
}

type serviceProposals struct {
	lockProposals   LockProposals
	unlockProposals UnlockProposals
}

func (s *serviceProposals) Lock() LockProposals {
	return s.lockProposals
}
func (s *serviceProposals) Unlock() UnlockProposals {
	return s.unlockProposals
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

type LockProposals interface {
	Get(ProposalID) (LockProposal, bool)
	List() []LockProposal
}

func newLockProposals(commands rsm.Commands) LockProposals {
	return &lockProposals{
		commands: commands,
	}
}

type lockProposals struct {
	commands rsm.Commands
}

func (p *lockProposals) Get(id ProposalID) (LockProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	return newLockProposal(command), true
}

func (p *lockProposals) List() []LockProposal {
	commands := p.commands.List(rsm.OperationID(1))
	proposals := make([]LockProposal, len(commands))
	for i, command := range commands {
		proposals[i] = newLockProposal(command)
	}
	return proposals
}

var _ LockProposals = &lockProposals{}

type LockProposal interface {
	Proposal
	Request() (*lock.LockRequest, error)
	Reply(*lock.LockResponse) error
	Fail(error) error
	Close() error
}

func newLockProposal(command rsm.Command) LockProposal {
	return &lockProposal{
		Proposal: newProposal(command),
		command:  command,
	}
}

type lockProposal struct {
	Proposal
	command  rsm.Command
	complete bool
}

func (p *lockProposal) Request() (*lock.LockRequest, error) {
	request := &lock.LockRequest{}
	if err := proto.Unmarshal(p.command.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received LockProposal %s: %s", p, request)
	return request, nil
}

func (p *lockProposal) Reply(response *lock.LockResponse) error {
	if p.complete {
		return errors.NewConflict("reply already sent")
	}
	log.Debugf("Sending LockProposal %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.command.Output(output, nil)
	p.command.Close()
	p.complete = true
	return nil
}

func (p *lockProposal) Fail(err error) error {
	if p.complete {
		return errors.NewConflict("reply already sent")
	}
	log.Debugf("Sending LockProposal %s: %s", p, err)
	p.command.Output(nil, err)
	p.command.Close()
	p.complete = true
	return nil
}

func (p *lockProposal) Close() error {
	if p.complete {
		return errors.NewConflict("reply already sent")
	}
	p.complete = true
	p.command.Close()
	return nil
}

func (p *lockProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ LockProposal = &lockProposal{}

type UnlockProposals interface {
	Get(ProposalID) (UnlockProposal, bool)
	List() []UnlockProposal
}

func newUnlockProposals(commands rsm.Commands) UnlockProposals {
	return &unlockProposals{
		commands: commands,
	}
}

type unlockProposals struct {
	commands rsm.Commands
}

func (p *unlockProposals) Get(id ProposalID) (UnlockProposal, bool) {
	command, ok := p.commands.Get(rsm.CommandID(id))
	if !ok {
		return nil, false
	}
	return newUnlockProposal(command), true
}

func (p *unlockProposals) List() []UnlockProposal {
	commands := p.commands.List(rsm.OperationID(2))
	proposals := make([]UnlockProposal, len(commands))
	for i, command := range commands {
		proposals[i] = newUnlockProposal(command)
	}
	return proposals
}

var _ UnlockProposals = &unlockProposals{}

type UnlockProposal interface {
	Proposal
	Request() (*lock.UnlockRequest, error)
	Reply(*lock.UnlockResponse) error
}

func newUnlockProposal(command rsm.Command) UnlockProposal {
	return &unlockProposal{
		Proposal: newProposal(command),
		command:  command,
	}
}

type unlockProposal struct {
	Proposal
	command rsm.Command
}

func (p *unlockProposal) Request() (*lock.UnlockRequest, error) {
	request := &lock.UnlockRequest{}
	if err := proto.Unmarshal(p.command.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received UnlockProposal %s: %s", p, request)
	return request, nil
}

func (p *unlockProposal) Reply(response *lock.UnlockResponse) error {
	log.Debugf("Sending UnlockProposal %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.command.Output(output, nil)
	p.command.Close()
	return nil
}

func (p *unlockProposal) String() string {
	return fmt.Sprintf("ProposalID=%d, SessionID=%d", p.ID(), p.Session().ID())
}

var _ UnlockProposal = &unlockProposal{}

type GetLockQuery interface {
	Query
	Request() (*lock.GetLockRequest, error)
	Reply(*lock.GetLockResponse) error
}

func newGetLockQuery(query rsm.Query) GetLockQuery {
	return &getLockQuery{
		Query: newQuery(query),
		query: query,
	}
}

type getLockQuery struct {
	Query
	query rsm.Query
}

func (p *getLockQuery) Request() (*lock.GetLockRequest, error) {
	request := &lock.GetLockRequest{}
	if err := proto.Unmarshal(p.query.Input(), request); err != nil {
		return nil, err
	}
	log.Debugf("Received GetLockQuery %s: %s", p, request)
	return request, nil
}

func (p *getLockQuery) Reply(response *lock.GetLockResponse) error {
	log.Debugf("Sending GetLockQuery %s: %s", p, response)
	output, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	p.query.Output(output, nil)
	p.query.Close()
	return nil
}

func (p *getLockQuery) String() string {
	return fmt.Sprintf("SessionID=%d", p.Session().ID())
}

var _ GetLockQuery = &getLockQuery{}
