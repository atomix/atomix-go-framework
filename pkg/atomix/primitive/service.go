// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package primitive

import (
	"io"
)

// ID is a service identifier
type ID ServiceId

// ServiceContext provides information about the context within which a service is running
type ServiceContext interface {
	ProtocolContext

	// ServiceID is the service identifier
	ServiceID() ID
}

func newServiceContext(ctx ProtocolContext, id ServiceId) ServiceContext {
	return &serviceContext{
		ProtocolContext: ctx,
		id:      ID(id),
	}
}

// serviceContext is a default implementation of the service context
type serviceContext struct {
	ProtocolContext
	id ID
}

func (c *serviceContext) ServiceID() ID {
	return c.id
}

var _ ServiceContext = &serviceContext{}

// SessionOpen is an interface for listening to session open events
type SessionOpen interface {
	// SessionOpen is called when a session is opened for a service
	SessionOpen(*Session)
}

// SessionClosed is an interface for listening to session closed events
type SessionClosed interface {
	// SessionClosed is called when a session is closed for a service
	SessionClosed(*Session)
}

// SessionExpired is an interface for listening to session expired events
type SessionExpired interface {
	// SessionExpired is called when a session is expired for a service
	SessionExpired(*Session)
}

// Service is a primitive service
type Service interface {
	internalService

	// Backup is called to take a snapshot of the service state
	Backup(writer io.Writer) error

	// Resotre is called to restore the service state from a snapshot
	Restore(reader io.Reader) error
}

type internalService interface {
	Type() ServiceType
	setCurrentSession(*Session)
	addSession(*Session)
	removeSession(*Session)
	getOperation(name string) Operation
}

// NewManagedService creates a new primitive service
func NewManagedService(serviceType ServiceType, scheduler Scheduler, context ServiceContext) *ManagedService {
	return &ManagedService{
		serviceType: serviceType,
		Executor:    newExecutor(),
		Scheduler:   scheduler,
		Context:     context,
		sessions:    make(map[uint64]*Session),
	}
}

// ManagedService is a service that is managed by the service manager
type ManagedService struct {
	Executor       Executor
	Context        ServiceContext
	Scheduler      Scheduler
	serviceType    ServiceType
	sessions       map[uint64]*Session
	currentSession *Session
}

// Type returns the service type
func (s *ManagedService) Type() ServiceType {
	return s.serviceType
}

// Session returns the current session
func (s *ManagedService) Session() *Session {
	return s.currentSession
}

// SessionOf returns the session with the given identifier
func (s *ManagedService) SessionOf(id uint64) *Session {
	return s.sessions[id]
}

// Sessions returns a list of open sessions
func (s *ManagedService) Sessions() []*Session {
	sessions := make([]*Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}

// setCurrentSession sets the current session
func (s *ManagedService) setCurrentSession(session *Session) {
	s.currentSession = session
}

// addSession adds a session to the service
func (s *ManagedService) addSession(session *Session) {
	s.sessions[session.ID] = session
}

// removeSession removes a session from the service
func (s *ManagedService) removeSession(session *Session) {
	delete(s.sessions, session.ID)
}

// getOperation gets a service operation
func (s *ManagedService) getOperation(name string) Operation {
	return s.Executor.GetOperation(name)
}
