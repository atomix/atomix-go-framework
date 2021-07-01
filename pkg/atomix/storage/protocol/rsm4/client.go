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
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
	"time"
)

// ClientID is a client identifier
type ClientID uint64

func newClientState(manager *primitiveManagerState, clientID ClientID) *clientState {
	return &clientState{
		manager:  manager,
		clientID: clientID,
		sessions: make(map[SessionID]*sessionState),
	}
}

type clientState struct {
	manager        *primitiveManagerState
	clientID       ClientID
	sessionTimeout time.Duration
	lastUpdated    time.Time
	sessions       map[SessionID]*sessionState
}

func (s *clientState) snapshot() (*ClientSnapshot, error) {
	sessionSnapshots := make([]*SessionSnapshot, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessionSnapshot, err := session.snapshot()
		if err != nil {
			return nil, err
		}
		sessionSnapshots = append(sessionSnapshots, sessionSnapshot)
	}
	return &ClientSnapshot{
		ClientID:       s.clientID,
		SessionTimeout: s.sessionTimeout,
		LastUpdated:    s.lastUpdated,
		Sessions:       sessionSnapshots,
	}, nil
}

func (s *clientState) restore(snapshot *ClientSnapshot) error {
	s.clientID = snapshot.ClientID
	s.sessionTimeout = snapshot.SessionTimeout
	s.lastUpdated = snapshot.LastUpdated
	s.sessions = make(map[SessionID]*sessionState)
	for _, sessionSnapshot := range snapshot.Sessions {
		session := newSessionState(s, sessionSnapshot.SessionID)
		if err := session.restore(sessionSnapshot); err != nil {
			return err
		}
		s.sessions[session.ID()] = session
	}
	return nil
}

func (s *clientState) connect(request *ClientConnectRequest, stream streams.WriteStream) {
	s.sessionTimeout = request.SessionTimeout
	s.lastUpdated = s.manager.timestamp
	s.manager.clients[s.clientID] = s
	stream.Value(&ClientConnectResponse{
		ClientID: s.clientID,
	})
	stream.Close()
}

func (s *clientState) keepAlive(request *ClientKeepAliveRequest, stream streams.WriteStream) {
	s.lastUpdated = s.manager.timestamp
	for _, sessionRequest := range request.Sessions {
		session, ok := s.sessions[sessionRequest.SessionID]
		if ok {
			session.keepAlive(sessionRequest)
		}
	}
	stream.Value(&ClientKeepAliveResponse{})
	stream.Close()
}

func (s *clientState) close(request *ClientCloseRequest, stream streams.WriteStream) {
	delete(s.manager.clients, s.clientID)
	stream.Value(&ClientCloseResponse{})
	stream.Close()
}

func (s *clientState) command(request *ClientCommandRequest, stream streams.WriteStream) {
	switch r := request.Request.(type) {
	case *ClientCommandRequest_SessionOpen:
		s.sessionOpen(r.SessionOpen, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &ClientCommandResponse{
				Response: &ClientCommandResponse_SessionOpen{
					SessionOpen: value.(*SessionOpenResponse),
				},
			}, nil
		}))
	case *ClientCommandRequest_SessionClose:
		s.sessionClose(r.SessionClose, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &ClientCommandResponse{
				Response: &ClientCommandResponse_SessionClose{
					SessionClose: value.(*SessionCloseResponse),
				},
			}, nil
		}))
	case *ClientCommandRequest_SessionCommand:
		s.sessionCommand(r.SessionCommand, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &ClientCommandResponse{
				Response: &ClientCommandResponse_SessionCommand{
					SessionCommand: value.(*SessionCommandResponse),
				},
			}, nil
		}))
	}
}

func (s *clientState) sessionOpen(request *SessionOpenRequest, stream streams.WriteStream) {
	sessionID := SessionID(s.manager.index)
	session := newSessionState(s, sessionID)
	session.open(request, stream)
}

func (s *clientState) sessionClose(request *SessionCloseRequest, stream streams.WriteStream) {
	session, ok := s.sessions[request.SessionID]
	if !ok {
		stream.Error(errors.NewNotFound("session not found"))
		stream.Close()
		return
	}
	session.close(request, stream)
}

func (s *clientState) sessionCommand(request *SessionCommandRequest, stream streams.WriteStream) {
	session, ok := s.sessions[request.SessionID]
	if !ok {
		stream.Error(errors.NewNotFound("session not found"))
		stream.Close()
		return
	}
	session.command(request, stream)
}

func (s *clientState) query(request *ClientQueryRequest, stream streams.WriteStream) {
	switch r := request.Request.(type) {
	case *ClientQueryRequest_SessionQuery:
		s.sessionQuery(r.SessionQuery, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &ClientQueryResponse{
				Response: &ClientQueryResponse_SessionQuery{
					SessionQuery: value.(*SessionQueryResponse),
				},
			}, nil
		}))
	}
}

func (s *clientState) sessionQuery(request *SessionQueryRequest, stream streams.WriteStream) {
	session, ok := s.sessions[request.SessionID]
	if !ok {
		stream.Error(errors.NewNotFound("session not found"))
		stream.Close()
		return
	}
	session.query(request, stream)
}
