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
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"io"
	"time"
)

// ServiceType is a service type name
type ServiceType string

// ServiceID is a service identifier
type ServiceID uint64

// Index is a service index
type Index uint64

// ServiceContext is a service context
type ServiceContext interface {
	// ID returns the service identifier
	ID() ServiceID
	// Type returns the service type
	Type() ServiceType
	// Namespace returns the service namespace
	Namespace() string
	// Name returns the service name
	Name() string
	// Index returns the current service index
	Index() Index
	// Time returns the current service time
	Time() time.Time
	// Scheduler returns the service scheduler
	Scheduler() Scheduler
	// Sessions returns the open sessions
	Sessions() Sessions
	// Commands returns the pending commands
	Commands() Commands
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
	// ExecuteCommand executes a service command
	ExecuteCommand(Command)
	// ExecuteQuery executes a service query
	ExecuteQuery(Query)
}

func newService(manager *primitiveServiceManager) *primitiveService {
	return &primitiveService{
		manager: manager,
	}
}

type primitiveService struct {
	manager   *primitiveServiceManager
	info      ServiceInfo
	serviceID ServiceID
	service   Service
	sessions  *primitiveServiceSessions
	commands  *primitiveCommands
}

func (s *primitiveService) ID() ServiceID {
	return s.serviceID
}

func (s *primitiveService) Type() ServiceType {
	return s.info.Type
}

func (s *primitiveService) Namespace() string {
	return s.info.Namespace
}

func (s *primitiveService) Name() string {
	return s.info.Name
}

func (s *primitiveService) Index() Index {
	return s.manager.index
}

func (s *primitiveService) Time() time.Time {
	return s.manager.timestamp
}

func (s *primitiveService) Scheduler() Scheduler {
	return s.manager.scheduler
}

func (s *primitiveService) Sessions() Sessions {
	return s.sessions
}

func (s *primitiveService) Commands() Commands {
	return s.commands
}

func (s *primitiveService) open(serviceID ServiceID, info ServiceInfo) error {
	log.Debugf("Open service %d (%s:%s/%s)", serviceID, info.Type, info.Namespace, info.Name)
	s.serviceID = serviceID
	serviceType := s.manager.registry.GetService(info.Type)
	if serviceType == nil {
		return errors.NewInvalid("unknown service type '%s'", info.Type)
	}
	s.info = info
	s.commands = newCommands()
	s.sessions = newServiceSessions()
	s.service = serviceType(s)
	s.manager.services[s.serviceID] = s
	return nil
}

func (s *primitiveService) snapshot() (*ServiceSnapshot, error) {
	log.Debugf("Snapshot service %d", s.serviceID)
	sessions := make([]*ServiceSessionSnapshot, 0, len(s.sessions.sessions))
	for _, session := range s.sessions.sessions {
		sessionSnapshot, err := session.snapshot()
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, sessionSnapshot)
	}

	var b bytes.Buffer
	if err := s.service.Backup(&b); err != nil {
		log.Error(err)
		return nil, err
	}
	data := b.Bytes()

	return &ServiceSnapshot{
		ServiceID:   s.serviceID,
		ServiceInfo: s.info,
		Data:        data,
		Sessions:    sessions,
	}, nil
}

func (s *primitiveService) restore(snapshot *ServiceSnapshot) error {
	log.Debugf("Restore service %d", snapshot.ServiceID)
	s.serviceID = snapshot.ServiceID
	s.info = snapshot.ServiceInfo
	serviceType := s.manager.registry.GetService(snapshot.Type)
	if serviceType == nil {
		return errors.NewInvalid("unknown service type '%s'", snapshot.Type)
	}
	s.commands = newCommands()
	s.sessions = newServiceSessions()
	s.service = serviceType(s)
	for _, sessionSnapshot := range snapshot.Sessions {
		serviceSession := newServiceSession(s)
		if err := serviceSession.restore(sessionSnapshot); err != nil {
			return err
		}
	}
	err := s.service.Restore(bytes.NewReader(snapshot.Data))
	if err != nil {
		log.Error(err)
		return err
	}
	s.manager.services[s.serviceID] = s
	return nil
}
