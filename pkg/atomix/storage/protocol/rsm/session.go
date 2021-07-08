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

type SessionState int

const (
	SessionClosed SessionState = iota
	SessionOpen
)

// SessionID is a session identifier
type SessionID uint64

// Sessions provides access to open sessions
type Sessions interface {
	Get(SessionID) (Session, bool)
	List() []Session
	open(Session)
	close(Session)
}

// Session is a service session
type Session interface {
	// ID returns the session identifier
	ID() SessionID
	// State returns the current session state
	State() SessionState
	// Watch watches the session state
	Watch(f func(SessionState)) SessionStateWatcher
	// Commands returns the session commands
	Commands() Commands
}

// SessionStateWatcher is a context for a session state Watch call
type SessionStateWatcher interface {
	// Cancel cancels the watcher
	Cancel()
}
