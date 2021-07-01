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

package rsm

import (
	"bytes"
	"io"
	"time"
)

// ServiceID is a service identifier
type ServiceID ServiceId

// Index is a partition log index
type Index uint64

// ServiceContext provides information about the context within which a service is running
type ServiceContext interface {
	// Index returns the current index of the service
	Index() Index

	// Timestamp returns a deterministic, monotonically increasing timestamp
	Timestamp() time.Time

	// ServiceID is the service identifier
	ServiceID() ServiceID

	// ServiceType returns the service type
	ServiceType() string

	// Session returns the session with the given identifier
	Session(id SessionID) Session

	// Sessions returns a list of open sessions
	Sessions() []Session

	// Scheduler returns the service scheduler
	Scheduler() Scheduler

	// Operations returns the operations executor
	Operations() OperationRegistry
}

type serviceManagerContext struct {
	serviceID  ServiceID
	sessions   map[SessionID]Session
	index      Index
	timestamp  time.Time
	scheduler  Scheduler
	operations OperationRegistry
}

func (s *serviceManagerContext) Index() Index {
	return s.index
}

func (s *serviceManagerContext) setIndex(index Index) {
	s.index = index
}

func (s *serviceManagerContext) Timestamp() time.Time {
	return s.timestamp
}

func (s *serviceManagerContext) setTimestamp(timestamp time.Time) {
	s.timestamp = timestamp
}

func (s *serviceManagerContext) ServiceID() ServiceID {
	return s.serviceID
}

func (s *serviceManagerContext) ServiceType() string {
	return s.serviceID.Type
}

func (s *serviceManagerContext) Scheduler() Scheduler {
	return s.scheduler
}

func (s *serviceManagerContext) Operations() OperationRegistry {
	return s.operations
}

func (s *serviceManagerContext) Session(id SessionID) Session {
	return s.sessions[id]
}

func (s *serviceManagerContext) Sessions() []Session {
	sessions := make([]Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}

// addSession adds a session to the service
func (s *serviceManagerContext) addSession(session Session) {
	s.sessions[session.ID()] = session
}

// removeSession removes a session from the service
func (s *serviceManagerContext) removeSession(session Session) {
	delete(s.sessions, session.ID())
}

func newService(manager *stateManager, serviceID ServiceID) *serviceManager {
	context := &serviceManagerContext{
		serviceID:  serviceID,
		sessions:   make(map[SessionID]Session),
		scheduler:  newScheduler(),
		operations: newOperationRegistry(),
	}
	return &serviceManager{
		manager:               manager,
		serviceManagerContext: context,
		service:               manager.registry.GetService(serviceID.Type)(manager.scheduler, context),
	}
}

type serviceManager struct {
	*serviceManagerContext
	manager *stateManager
	service Service
}

// addSession adds a session to the service
func (s *serviceManager) addSession(session Session) {
	s.serviceManagerContext.addSession(session)
	if open, ok := s.service.(SessionOpenService); ok {
		open.SessionOpen(session)
	}
}

// removeSession removes a session from the service
func (s *serviceManager) removeSession(session Session) {
	s.serviceManagerContext.removeSession(session)
	if closed, ok := s.service.(SessionClosedService); ok {
		closed.SessionClosed(session)
	}
}

// expireSession expires a session from the service
func (s *serviceManager) expireSession(session Session) {
	s.serviceManagerContext.removeSession(session)
	if expired, ok := s.service.(SessionExpiredService); ok {
		expired.SessionExpired(session)
	}
}

func (s *serviceManager) snapshot() (*ServiceSnapshot, error) {
	var b bytes.Buffer
	if err := s.service.Backup(&b); err != nil {
		return nil, err
	}
	return &ServiceSnapshot{
		ServiceID: s.serviceID,
		Index:     s.index,
		Data:      b.Bytes(),
	}, nil
}

func (s *serviceManager) restore(snapshot *ServiceSnapshot) error {
	s.serviceID = snapshot.ServiceID
	s.index = snapshot.Index
	if err := s.service.Restore(bytes.NewReader(snapshot.Data)); err != nil {
		return err
	}
	return nil
}

// SessionOpenService is an interface for listening to session open events
type SessionOpenService interface {
	// SessionOpen is called when a session is opened for a service
	SessionOpen(Session)
}

// SessionClosedService is an interface for listening to session closed events
type SessionClosedService interface {
	// SessionClosed is called when a session is closed for a service
	SessionClosed(Session)
}

// SessionExpiredService is an interface for listening to session expired events
type SessionExpiredService interface {
	// SessionExpired is called when a session is expired for a service
	SessionExpired(Session)
}

// BackupService is an interface for backing up a service
type BackupService interface {
	// Backup is called to take a snapshot of the service state
	Backup(writer io.Writer) error
}

// RestoreService is an interface for restoring up a service
type RestoreService interface {
	// Restore is called to restore the service state from a snapshot
	Restore(reader io.Reader) error
}

// Service is a primitive service
type Service interface {
	BackupService
	RestoreService
	ServiceContext
}
