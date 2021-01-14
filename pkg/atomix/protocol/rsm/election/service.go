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

package election

import (
	"github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/api/go/atomix/primitive/meta"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &electionService{
		Service: rsm.NewService(scheduler, context),
		term:    &election.Term{},
	}
}

// electionService is a state machine for an election primitive
type electionService struct {
	rsm.Service
	term    *election.Term
	streams []ServiceEventsStream
}

// SessionExpired is called when a session is expired by the server
func (e *electionService) SessionExpired(session rsm.Session) {
	e.close(session)
}

// SessionClosed is called when a session is closed by the client
func (e *electionService) SessionClosed(session rsm.Session) {
	e.close(session)
}

// close elects a new leader when a session is closed
func (e *electionService) close(session rsm.Session) {
	newCandidates := make([]string, 0, len(e.term.Candidates))
	for _, candidate := range e.term.Candidates {
		if rsm.ClientID(candidate) != session.ClientID() {
			newCandidates = append(newCandidates, candidate)
		}
	}
	e.updateTerm(newCandidates)
}

func (e *electionService) notify(event *election.EventsOutput) error {
	for _, stream := range e.streams {
		if err := stream.Notify(event); err != nil {
			return err
		}
	}
	return nil
}

func (e *electionService) updateTerm(newCandidates []string) (*election.Term, error) {
	oldTerm := e.term
	if slicesMatch(oldTerm.Candidates, newCandidates) {
		return e.term, nil
	}

	newTerm := &election.Term{}
	if len(newCandidates) == 0 {
		newTerm.Meta.Revision = oldTerm.Meta.Revision
	} else {
		newTerm.Leader = newCandidates[0]
		if oldTerm.Leader != newTerm.Leader {
			newTerm.Meta.Revision = &meta.Revision{
				Num: oldTerm.Meta.Revision.Num + 1,
			}
		} else {
			newTerm.Meta.Revision = oldTerm.Meta.Revision
		}
	}

	newTerm.Meta.Timestamp = &meta.Timestamp{
		Timestamp: &meta.Timestamp_PhysicalTimestamp{
			PhysicalTimestamp: &meta.PhysicalTimestamp{
				Time: e.Timestamp(),
			},
		},
	}
	e.term = newTerm
	err := e.notify(&election.EventsOutput{
		Term: newTerm,
	})
	if err != nil {
		return nil, err
	}
	return newTerm, nil
}

func (e *electionService) Enter(input *election.EnterInput) (*election.EnterOutput, error) {
	clientID := string(e.CurrentSession().ClientID())
	candidates := e.term.Candidates[:]
	if !sliceContains(candidates, clientID) {
		candidates = append(candidates, clientID)
	}

	term, err := e.updateTerm(candidates)
	if err != nil {
		return nil, err
	}

	return &election.EnterOutput{
		Term: term,
	}, nil
}

func (e *electionService) Withdraw(input *election.WithdrawInput) (*election.WithdrawOutput, error) {
	clientID := string(e.CurrentSession().ClientID())
	candidates := make([]string, 0, len(e.term.Candidates))
	for _, candidate := range e.term.Candidates {
		if candidate != clientID {
			candidates = append(candidates, candidate)
		}
	}

	term, err := e.updateTerm(candidates)
	if err != nil {
		return nil, err
	}

	return &election.WithdrawOutput{
		Term: term,
	}, nil
}

func (e *electionService) Anoint(input *election.AnointInput) (*election.AnointOutput, error) {
	clientID := string(e.CurrentSession().ClientID())
	if !sliceContains(e.term.Candidates, clientID) {
		return nil, errors.NewInvalid("not a candidate")
	}

	candidates := make([]string, 0, len(e.term.Candidates))
	candidates = append(candidates, clientID)
	for _, candidate := range e.term.Candidates {
		if candidate != clientID {
			candidates = append(candidates, candidate)
		}
	}

	term, err := e.updateTerm(candidates)
	if err != nil {
		return nil, err
	}
	return &election.AnointOutput{
		Term: term,
	}, nil
}

func (e *electionService) Promote(input *election.PromoteInput) (*election.PromoteOutput, error) {
	clientID := string(e.CurrentSession().ClientID())
	if !sliceContains(e.term.Candidates, clientID) {
		return nil, errors.NewInvalid("not a candidate")
	}

	candidates := make([]string, 0, len(e.term.Candidates))
	var index int
	for i, candidate := range e.term.Candidates {
		if candidate == clientID {
			index = i
			break
		}
	}

	candidates = append(candidates, clientID)
	for i, candidate := range e.term.Candidates {
		if i < index-1 {
			candidates[i] = candidate
		} else if i == index-1 {
			candidates[i] = clientID
		} else if i == index {
			candidates[i] = e.term.Candidates[i-1]
		} else {
			candidates[i] = candidate
		}
	}

	term, err := e.updateTerm(candidates)
	if err != nil {
		return nil, err
	}
	return &election.PromoteOutput{
		Term: term,
	}, nil
}

func (e *electionService) Evict(input *election.EvictInput) (*election.EvictOutput, error) {
	clientID := input.CandidateID
	if !sliceContains(e.term.Candidates, clientID) {
		return nil, errors.NewInvalid("not a candidate")
	}

	candidates := make([]string, 0, len(e.term.Candidates))
	for _, candidate := range e.term.Candidates {
		if candidate != clientID {
			candidates = append(candidates, candidate)
		}
	}

	term, err := e.updateTerm(candidates)
	if err != nil {
		return nil, err
	}
	return &election.EvictOutput{
		Term: term,
	}, nil
}

func (e *electionService) GetTerm(input *election.GetTermInput) (*election.GetTermOutput, error) {
	return &election.GetTermOutput{
		Term: e.term,
	}, nil
}

func (e *electionService) Events(input *election.EventsInput, stream ServiceEventsStream) error {
	e.streams = append(e.streams, stream)
	return nil
}

func (e *electionService) Snapshot() (*election.Snapshot, error) {
	return &election.Snapshot{
		Term: e.term,
	}, nil
}

func (e *electionService) Restore(snapshot *election.Snapshot) error {
	e.term = snapshot.Term
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
