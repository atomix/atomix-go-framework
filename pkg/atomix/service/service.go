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

type SessionOpen interface {
	SessionOpen(*Session)
}

type SessionClosed interface {
	SessionClosed(*Session)
}

type SessionExpired interface {
	SessionExpired(*Session)
}

type Service interface {
	internalService
	Backup(writer io.Writer) error
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

func (s *ManagedService) Type() string {
	return s.serviceType
}

func (s *ManagedService) Session() *Session {
	return s.currentSession
}

func (s *ManagedService) Sessions() []*Session {
	return s.sessions
}

func (s *ManagedService) setScheduler(scheduler Scheduler) {
	s.Scheduler = scheduler
}

func (s *ManagedService) setCurrentSession(session *Session) {
	s.currentSession = session
}

func (s *ManagedService) addSession(session *Session) {
	s.sessions[session.ID] = session
}

func (s *ManagedService) removeSession(session *Session) {
	sessions := make([]*Session, 0, len(s.sessions)-1)
	for _, sess := range s.sessions {
		if sess.ID != session.ID {
			sessions = append(sessions, sess)
		}
	}
	s.sessions = sessions
}

func (s *ManagedService) getOperation(name string) Operation {
	return s.Executor.GetOperation(name)
}
