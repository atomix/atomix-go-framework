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
	"fmt"
	leaderapi "github.com/atomix/atomix-api/go/atomix/primitive/leader"
	"github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(context ServiceContext) Service {
	return &leaderService{
		ServiceContext: context,
	}
}

// leaderService is a state machine for an election primitive
type leaderService struct {
	ServiceContext
	latch leaderapi.Latch
}

func (l *leaderService) SetState(state *LeaderLatchState) error {
	return nil
}

func (l *leaderService) GetState() (*LeaderLatchState, error) {
	return &LeaderLatchState{}, nil
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

func (l *leaderService) notify(event leaderapi.Event) error {
	output := &leaderapi.EventsResponse{
		Event: event,
	}
	for _, events := range l.Proposals().Events().List() {
		if err := events.Notify(output); err != nil {
			return err
		}
	}
	return nil
}

func (l *leaderService) updateLatch(newParticipants []string) (leaderapi.Latch, error) {
	oldLatch := l.latch
	if slicesMatch(oldLatch.Participants, newParticipants) {
		return l.latch, nil
	}

	var newLatch leaderapi.Latch
	if len(newParticipants) == 0 {
		newLatch.ObjectMeta.Revision = oldLatch.ObjectMeta.Revision
	} else {
		newLatch.Leader = newParticipants[0]
		if oldLatch.Leader != newLatch.Leader {
			newLatch.ObjectMeta.Revision = &meta.Revision{
				Num: oldLatch.ObjectMeta.Revision.Num + 1,
			}
		} else {
			newLatch.ObjectMeta.Revision = oldLatch.ObjectMeta.Revision
		}
	}

	newLatch.ObjectMeta.Timestamp = &meta.Timestamp{
		Timestamp: &meta.Timestamp_PhysicalTimestamp{
			PhysicalTimestamp: &meta.PhysicalTimestamp{
				Time: l.Scheduler().Time(),
			},
		},
	}
	l.latch = newLatch
	err := l.notify(leaderapi.Event{
		Type:  leaderapi.Event_CHANGE,
		Latch: newLatch,
	})
	if err != nil {
		return leaderapi.Latch{}, err
	}
	return newLatch, nil
}

func (l *leaderService) Latch(proposal LatchProposal) error {
	clientID := fmt.Sprint(proposal.Session().ID())
	participants := l.latch.Participants[:]
	if !sliceContains(participants, clientID) {
		participants = append(participants, clientID)
	}

	latch, err := l.updateLatch(participants)
	if err != nil {
		return err
	}

	return proposal.Reply(&leaderapi.LatchResponse{
		Latch: latch,
	})
}

func (l *leaderService) Get(get GetProposal) error {
	return get.Reply(&leaderapi.GetResponse{
		Latch: l.latch,
	})
}

func (l *leaderService) Events(events EventsProposal) error {
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
