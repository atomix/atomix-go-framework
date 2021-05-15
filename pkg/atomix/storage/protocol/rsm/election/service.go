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
	electionapi "github.com/atomix/atomix-api/go/atomix/primitive/election"
	"github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
)

func init() {
	registerServiceFunc(newService)
}

func newService(context ServiceContext) Service {
	return &electionService{
		ServiceContext: context,
		watchers:       make(map[SessionID]Watcher),
		term: electionapi.Term{
			ObjectMeta: meta.ObjectMeta{
				Revision: &meta.Revision{},
			},
		},
	}
}

// electionService is a state machine for an election primitive
type electionService struct {
	ServiceContext
	watchers map[SessionID]Watcher
	term     electionapi.Term
}

func (e *electionService) Backup(writer SnapshotWriter) error {
	return writer.WriteState(&LeaderElectionState{})
}

func (e *electionService) Restore(reader SnapshotReader) error {
	_, err := reader.ReadState()
	if err != nil {
		return err
	}
	return nil
}

func (e *electionService) watchSession(candidateID string, session Session) {
	e.watchers[session.ID()] = session.Watch(func(state SessionState) {
		if state == SessionClosed {
			newCandidates := make([]string, 0, len(e.term.Candidates))
			for _, candidate := range e.term.Candidates {
				if candidate != candidateID {
					newCandidates = append(newCandidates, candidate)
				}
			}
			e.updateTerm(newCandidates)
			delete(e.watchers, session.ID())
		}
	})
}

func (e *electionService) notify(event electionapi.Event) error {
	output := &electionapi.EventsResponse{
		Event: event,
	}
	for _, events := range e.Proposals().Events().List() {
		if err := events.Notify(output); err != nil {
			return err
		}
	}
	return nil
}

func (e *electionService) updateTerm(newCandidates []string) (electionapi.Term, error) {
	oldTerm := e.term
	if slicesMatch(oldTerm.Candidates, newCandidates) {
		return e.term, nil
	}

	newTerm := electionapi.Term{
		ObjectMeta: meta.ObjectMeta{
			Timestamp: &meta.Timestamp{
				Timestamp: &meta.Timestamp_PhysicalTimestamp{
					PhysicalTimestamp: &meta.PhysicalTimestamp{
						Time: e.Scheduler().Time(),
					},
				},
			},
		},
		Candidates: newCandidates,
	}

	if len(newCandidates) == 0 {
		newTerm.ObjectMeta.Revision = oldTerm.ObjectMeta.Revision
	} else {
		newTerm.Leader = newCandidates[0]
		if oldTerm.Leader != newTerm.Leader {
			newTerm.ObjectMeta.Revision = &meta.Revision{
				Num: oldTerm.ObjectMeta.Revision.Num + 1,
			}
		} else {
			newTerm.ObjectMeta.Revision = oldTerm.ObjectMeta.Revision
		}
	}

	e.term = newTerm

	err := e.notify(electionapi.Event{
		Type: electionapi.Event_CHANGED,
		Term: newTerm,
	})
	if err != nil {
		return electionapi.Term{}, err
	}
	return newTerm, nil
}

func (e *electionService) Enter(enter EnterProposal) error {
	e.watchSession(enter.Request().CandidateID, enter.Session())

	candidates := e.term.Candidates[:]
	if !sliceContains(candidates, enter.Request().CandidateID) {
		candidates = append(candidates, enter.Request().CandidateID)
	}

	term, err := e.updateTerm(candidates)
	if err != nil {
		return err
	}

	return enter.Reply(&electionapi.EnterResponse{
		Term: term,
	})
}

func (e *electionService) Withdraw(withdraw WithdrawProposal) error {
	candidates := make([]string, 0, len(e.term.Candidates))
	for _, candidate := range e.term.Candidates {
		if candidate != withdraw.Request().CandidateID {
			candidates = append(candidates, candidate)
		}
	}

	term, err := e.updateTerm(candidates)
	if err != nil {
		return err
	}

	return withdraw.Reply(&electionapi.WithdrawResponse{
		Term: term,
	})
}

func (e *electionService) Anoint(anoint AnointProposal) error {
	if !sliceContains(e.term.Candidates, anoint.Request().CandidateID) {
		return errors.NewInvalid("not a candidate")
	}

	candidates := make([]string, 0, len(e.term.Candidates))
	candidates = append(candidates, anoint.Request().CandidateID)
	for _, candidate := range e.term.Candidates {
		if candidate != anoint.Request().CandidateID {
			candidates = append(candidates, candidate)
		}
	}

	term, err := e.updateTerm(candidates)
	if err != nil {
		return err
	}
	return anoint.Reply(&electionapi.AnointResponse{
		Term: term,
	})
}

func (e *electionService) Promote(promote PromoteProposal) error {
	if !sliceContains(e.term.Candidates, promote.Request().CandidateID) {
		return errors.NewInvalid("not a candidate")
	}

	var index int
	for i, candidate := range e.term.Candidates {
		if candidate == promote.Request().CandidateID {
			index = i
			break
		}
	}

	if index == 0 {
		return promote.Reply(&electionapi.PromoteResponse{
			Term: e.term,
		})
	}

	candidates := make([]string, len(e.term.Candidates))
	for i, candidate := range e.term.Candidates {
		if i < index-1 {
			candidates[i] = candidate
		} else if i == index-1 {
			candidates[i] = promote.Request().CandidateID
		} else if i == index {
			candidates[i] = e.term.Candidates[i-1]
		} else {
			candidates[i] = candidate
		}
	}

	term, err := e.updateTerm(candidates)
	if err != nil {
		return err
	}
	return promote.Reply(&electionapi.PromoteResponse{
		Term: term,
	})
}

func (e *electionService) Evict(evict EvictProposal) error {
	if !sliceContains(e.term.Candidates, evict.Request().CandidateID) {
		return errors.NewInvalid("not a candidate")
	}

	candidates := make([]string, 0, len(e.term.Candidates))
	for _, candidate := range e.term.Candidates {
		if candidate != evict.Request().CandidateID {
			candidates = append(candidates, candidate)
		}
	}

	term, err := e.updateTerm(candidates)
	if err != nil {
		return err
	}
	return evict.Reply(&electionapi.EvictResponse{
		Term: term,
	})
}

func (e *electionService) GetTerm(getTerm GetTermProposal) error {
	return getTerm.Reply(&electionapi.GetTermResponse{
		Term: e.term,
	})
}

func (e *electionService) Events(events EventsProposal) error {
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
