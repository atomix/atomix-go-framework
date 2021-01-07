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

package leader

import (
	"github.com/atomix/api/go/atomix/primitive/leader"
	"github.com/atomix/api/go/atomix/primitive/meta"
	"github.com/atomix/go-framework/pkg/atomix/storage/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &leaderService{
		Service: rsm.NewService(scheduler, context),
	}
}

// leaderService is a state machine for an election primitive
type leaderService struct {
	rsm.Service
	latch   leader.Latch
	streams []ServiceEventsStream
}

// SessionExpired is called when a session is expired by the server
func (l *leaderService) SessionExpired(session rsm.Session) {
	l.close(session)
}

// SessionClosed is called when a session is closed by the client
func (l *leaderService) SessionClosed(session rsm.Session) {
	l.close(session)
}

// close elects a new leader when a session is closed
func (l *leaderService) close(session rsm.Session) {
	participants := make([]string, 0, len(l.latch.Participants))
	for _, participant := range l.latch.Participants {
		if rsm.ClientID(participant) != session.ClientID() {
			participants = append(participants, participant)
		}
	}
	l.updateLatch(participants)
}

func (l *leaderService) notify(event *leader.EventsOutput) error {
	for _, stream := range l.streams {
		if err := stream.Notify(event); err != nil {
			return err
		}
	}
	return nil
}

func (l *leaderService) updateLatch(newParticipants []string) (leader.Latch, error) {
	oldLatch := l.latch
	if slicesMatch(oldLatch.Participants, newParticipants) {
		return l.latch, nil
	}

	var newLatch leader.Latch
	if len(newParticipants) == 0 {
		newLatch.ID = oldLatch.ID
	} else {
		newLatch.Leader = newParticipants[0]
		if oldLatch.Leader != newLatch.Leader {
			newLatch.ID = meta.Epoch{
				Value: oldLatch.ID.Value + 1,
			}
		} else {
			newLatch.ID = oldLatch.ID
		}
	}

	l.latch = newLatch
	err := l.notify(&leader.EventsOutput{
		Latch: &newLatch,
	})
	if err != nil {
		return leader.Latch{}, err
	}
	return newLatch, nil
}

func (l *leaderService) Latch(input *leader.LatchInput) (*leader.LatchOutput, error) {
	clientID := string(l.CurrentSession().ClientID())
	participants := l.latch.Participants[:]
	if !sliceContains(participants, clientID) {
		participants = append(participants, clientID)
	}

	latch, err := l.updateLatch(participants)
	if err != nil {
		return nil, err
	}

	return &leader.LatchOutput{
		Latch: &latch,
	}, nil
}

func (l *leaderService) Get(input *leader.GetInput) (*leader.GetOutput, error) {
	return &leader.GetOutput{
		Latch: &l.latch,
	}, nil
}

func (l *leaderService) Events(input *leader.EventsInput, stream ServiceEventsStream) error {
	l.streams = append(l.streams, stream)
	return nil
}

func (l *leaderService) Snapshot() (*leader.Snapshot, error) {
	return &leader.Snapshot{
		Latch: &l.latch,
	}, nil
}

func (l *leaderService) Restore(snapshot *leader.Snapshot) error {
	l.latch = leader.Latch{}
	if snapshot.Latch != nil {
		l.latch = *snapshot.Latch
	}
	return nil
}

func slicesMatch(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}

func sliceContains(s []string, value string) bool {
	for _, v := range s {
		if v == value {
			return true
		}
	}
	return false
}
