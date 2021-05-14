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
	"fmt"
	electionapi "github.com/atomix/atomix-api/go/atomix/primitive/election"
	"github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(context ServiceContext) Service {
	return &electionService{
		ServiceContext: context,
	}
}

// electionService is a state machine for an election primitive
type electionService struct {
	ServiceContext
	term electionapi.Term
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

	newTerm := electionapi.Term{}
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

	newTerm.ObjectMeta.Timestamp = &meta.Timestamp{
		Timestamp: &meta.Timestamp_PhysicalTimestamp{
			PhysicalTimestamp: &meta.PhysicalTimestamp{
				Time: e.Scheduler().Time(),
			},
		},
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
	clientID := fmt.Sprint(enter.Session().ID())
	candidates := e.term.Candidates[:]
	if !sliceContains(candidates, clientID) {
		candidates = append(candidates, clientID)
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
	clientID := fmt.Sprint(withdraw.Session().ID())
	candidates := make([]string, 0, len(e.term.Candidates))
	for _, candidate := range e.term.Candidates {
		if candidate != clientID {
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
	clientID := fmt.Sprint(anoint.Session().ID())
	if !sliceContains(e.term.Candidates, clientID) {
		return errors.NewInvalid("not a candidate")
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
		return err
	}
	return anoint.Reply(&electionapi.AnointResponse{
		Term: term,
	})
}

func (e *electionService) Promote(promote PromoteProposal) error {
	clientID := fmt.Sprint(promote.Session().ID())
	if !sliceContains(e.term.Candidates, clientID) {
		return errors.NewInvalid("not a candidate")
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
		return err
	}
	return promote.Reply(&electionapi.PromoteResponse{
		Term: term,
	})
}

func (e *electionService) Evict(evict EvictProposal) error {
	clientID := evict.Request().CandidateID
	if !sliceContains(e.term.Candidates, clientID) {
		return errors.NewInvalid("not a candidate")
	}

	candidates := make([]string, 0, len(e.term.Candidates))
	for _, candidate := range e.term.Candidates {
		if candidate != clientID {
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
