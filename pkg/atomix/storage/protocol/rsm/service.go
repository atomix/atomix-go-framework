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
	"io"
	"time"
)

// ServiceID is a service identifier
type ServiceID ServiceId

// Index is a service index
type Index uint64

// ServiceContext is a service context
type ServiceContext interface {
	// ID returns the service identifier
	ID() ServiceID
	// Index returns the current service index
	Index() Index
	setIndex(Index)
	// Time returns the current service time
	Time() time.Time
	setTime(time.Time)
	// Scheduler returns the service scheduler
	Scheduler() Scheduler
	// Sessions returns the open sessions
	Sessions() Sessions
	// Commands returns the pending commands
	Commands() Commands
}

// OpenSessionService is an interface for listening to session open events
type OpenSessionService interface {
	// OpenSession is called when a session is opened for a service
	OpenSession(Session)
}

// CloseSessionService is an interface for listening to session closed events
type CloseSessionService interface {
	// CloseSession is called when a session is closed for a service
	CloseSession(Session)
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
	ExecuteCommand(Command) error
	// ExecuteQuery executes a service query
	ExecuteQuery(Query) error
}
