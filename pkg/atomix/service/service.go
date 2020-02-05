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

package service

import (
	"io"
	"time"
)

// Context provides information about the context within which a service is running
type Context interface {
	// Node is the local node identifier
	Node() string

	// Index returns the current index of the service
	Index() uint64

	// Timestamp returns a deterministic, monotonically increasing timestamp
	Timestamp() time.Time
}

// Registry is a service registry
type Registry interface {
	// GetType returns a service type by name
	GetType(name string) func(scheduler Scheduler, context Context) Service
}

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
	Type() string
	setCurrentSession(*Session)
	addSession(*Session)
	removeSession(*Session)
	getOperation(name string) Operation
}

// NewManagedService creates a new primitive service
func NewManagedService(serviceType string, scheduler Scheduler, context Context) *ManagedService {
	return &ManagedService{
		serviceType: serviceType,
		Executor:    newExecutor(),
		Scheduler:   scheduler,
		Context:     context,
		sessions:    make([]*Session, 0),
	}
}

// ManagedService is a service that is managed by the service manager
type ManagedService struct {
	Executor       Executor
	Context        Context
	Scheduler      Scheduler
	serviceType    string
	sessions       []*Session
	currentSession *Session
}

// Type returns the service type
func (s *ManagedService) Type() string {
	return s.serviceType
}

// Session returns the current session
func (s *ManagedService) Session() *Session {
	return s.currentSession
}

// Sessions returns a list of open sessions
func (s *ManagedService) Sessions() []*Session {
	return s.sessions
}

// setCurrentSession sets the current session
func (s *ManagedService) setCurrentSession(session *Session) {
	s.currentSession = session
}

// addSession adds a session to the service
func (s *ManagedService) addSession(session *Session) {
	s.sessions = append(s.sessions, session)
}

// removeSession removes a session from the service
func (s *ManagedService) removeSession(session *Session) {
	sessions := make([]*Session, 0, len(s.sessions)-1)
	for _, sess := range s.sessions {
		if sess.ID != session.ID {
			sessions = append(sessions, sess)
		}
	}
	s.sessions = sessions
}

// getOperation gets a service operation
func (s *ManagedService) getOperation(name string) Operation {
	return s.Executor.GetOperation(name)
}
