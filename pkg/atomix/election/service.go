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
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/golang/protobuf/proto"
	"time"
)

// RegisterService registers the election service in the given service registry
func RegisterService(registry *service.Registry) {
	registry.Register("election", newService)
}

// newService returns a new Service
func newService(context service.Context) service.Service {
	service := &Service{
		SessionizedService: service.NewSessionizedService(context),
		candidates:         make([]*ElectionRegistration, 0),
	}
	service.init()
	return service
}

// Service is a state machine for an election primitive
type Service struct {
	*service.SessionizedService
	leader     *ElectionRegistration
	term       uint64
	timestamp  *time.Time
	candidates []*ElectionRegistration
}

// init initializes the election service
func (e *Service) init() {
	e.Executor.Register("Enter", e.Enter)
	e.Executor.Register("Withdraw", e.Withdraw)
	e.Executor.Register("Anoint", e.Anoint)
	e.Executor.Register("Promote", e.Promote)
	e.Executor.Register("Evict", e.Evict)
	e.Executor.Register("GetTerm", e.GetTerm)
	e.Executor.Register("Events", e.Events)
}

// Backup backs up the list service
func (e *Service) Backup() ([]byte, error) {
	snapshot := &ElectionSnapshot{
		Term:       e.term,
		Timestamp:  e.timestamp,
		Leader:     e.leader,
		Candidates: e.candidates,
	}
	return proto.Marshal(snapshot)
}

// Restore restores the list service
func (e *Service) Restore(bytes []byte) error {
	snapshot := &ElectionSnapshot{}
	if err := proto.Unmarshal(bytes, snapshot); err != nil {
		return err
	}
	e.term = snapshot.Term
	e.timestamp = snapshot.Timestamp
	e.leader = snapshot.Leader
	e.candidates = snapshot.Candidates
	return nil
}

// getTerm returns the current election term
func (e *Service) getTerm() *Term {
	var leader string
	if e.leader != nil {
		leader = e.leader.ID
	}
	return &Term{
		ID:         e.term,
		Timestamp:  e.timestamp,
		Leader:     leader,
		Candidates: e.getCandidates(),
	}
}

// getCandidates returns a slice of candidate IDs
func (e *Service) getCandidates() []string {
	candidates := make([]string, len(e.candidates))
	for i, candidate := range e.candidates {
		candidates[i] = candidate.ID
	}
	return candidates
}

// Enter enters a candidate in the election
func (e *Service) Enter(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &EnterRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- e.NewFailure(err)
		return
	}

	reg := &ElectionRegistration{
		ID:        request.ID,
		SessionID: e.Session().ID,
	}

	e.candidates = append(e.candidates, reg)
	if e.leader == nil {
		e.leader = reg
		e.term++
		timestamp := e.Context.Timestamp()
		e.timestamp = &timestamp
	}

	e.sendEvent(&ListenResponse{
		Type: ListenResponse_CHANGED,
		Term: e.getTerm(),
	})

	ch <- e.NewResult(proto.Marshal(&EnterResponse{
		Term: e.getTerm(),
	}))
}

// Withdraw withdraws a candidate from the election
func (e *Service) Withdraw(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &WithdrawRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- e.NewFailure(err)
		return
	}

	candidates := make([]*ElectionRegistration, 0, len(e.candidates))
	for _, candidate := range e.candidates {
		if candidate.ID != request.ID {
			candidates = append(candidates, candidate)
		}
	}

	if len(candidates) != len(e.candidates) {
		e.candidates = candidates

		if e.leader.ID == request.ID {
			e.leader = nil
			if len(e.candidates) > 0 {
				e.leader = e.candidates[0]
				e.term++
				timestamp := e.Context.Timestamp()
				e.timestamp = &timestamp
			}
		}

		e.sendEvent(&ListenResponse{
			Type: ListenResponse_CHANGED,
			Term: e.getTerm(),
		})
	}

	ch <- e.NewResult(proto.Marshal(&WithdrawResponse{
		Term: e.getTerm(),
	}))
}

// Anoint assigns leadership to a candidate
func (e *Service) Anoint(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &AnointRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- e.NewFailure(err)
		return
	}

	if e.leader != nil && e.leader.ID == request.ID {
		ch <- e.NewResult(proto.Marshal(&AnointResponse{
			Term: e.getTerm(),
		}))
		return
	}

	var leader *ElectionRegistration
	for _, candidate := range e.candidates {
		if candidate.ID == request.ID {
			leader = candidate
			break
		}
	}

	if leader == nil {
		ch <- e.NewResult(proto.Marshal(&AnointResponse{
			Term: e.getTerm(),
		}))
		return
	}

	candidates := make([]*ElectionRegistration, 0, len(e.candidates))
	candidates = append(candidates, leader)
	for _, candidate := range e.candidates {
		if candidate.ID != request.ID {
			candidates = append(candidates, candidate)
		}
	}

	e.leader = leader
	e.term++
	timestamp := e.Context.Timestamp()
	e.timestamp = &timestamp
	e.candidates = candidates

	e.sendEvent(&ListenResponse{
		Type: ListenResponse_CHANGED,
		Term: e.getTerm(),
	})

	ch <- e.NewResult(proto.Marshal(&AnointResponse{
		Term: e.getTerm(),
	}))
}

// Promote increases the priority of a candidate
func (e *Service) Promote(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &PromoteRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- e.NewFailure(err)
		return
	}

	if e.leader != nil && e.leader.ID == request.ID {
		ch <- e.NewResult(proto.Marshal(&PromoteResponse{
			Term: e.getTerm(),
		}))
		return
	}

	var index int
	var promote *ElectionRegistration
	for i, candidate := range e.candidates {
		if candidate.ID == request.ID {
			index = i
			promote = candidate
			break
		}
	}

	if promote == nil {
		ch <- e.NewResult(proto.Marshal(&PromoteResponse{
			Term: e.getTerm(),
		}))
		return
	}

	candidates := make([]*ElectionRegistration, len(e.candidates))
	for i, candidate := range e.candidates {
		if i < index-1 {
			candidates[i] = candidate
		} else if i == index-1 {
			candidates[i] = promote
		} else if i == index {
			candidates[i] = e.candidates[i-1]
		} else {
			candidates[i] = candidate
		}
	}

	leader := candidates[0]
	if e.leader.ID != leader.ID {
		e.leader = leader
		e.term++
		timestamp := e.Context.Timestamp()
		e.timestamp = &timestamp
	}
	e.candidates = candidates

	e.sendEvent(&ListenResponse{
		Type: ListenResponse_CHANGED,
		Term: e.getTerm(),
	})

	ch <- e.NewResult(proto.Marshal(&AnointResponse{
		Term: e.getTerm(),
	}))
}

// Evict removes a candidate from the election
func (e *Service) Evict(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &EvictRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- e.NewFailure(err)
		return
	}

	candidates := make([]*ElectionRegistration, 0, len(e.candidates))
	for _, candidate := range e.candidates {
		if candidate.ID != request.ID {
			candidates = append(candidates, candidate)
		}
	}

	if len(candidates) != len(e.candidates) {
		e.candidates = candidates

		if e.leader.ID == request.ID {
			e.leader = nil
			if len(e.candidates) > 0 {
				e.leader = e.candidates[0]
				e.term++
				timestamp := e.Context.Timestamp()
				e.timestamp = &timestamp
			}
		}

		e.sendEvent(&ListenResponse{
			Type: ListenResponse_CHANGED,
			Term: e.getTerm(),
		})
	}

	ch <- e.NewResult(proto.Marshal(&WithdrawResponse{
		Term: e.getTerm(),
	}))
}

// GetTerm gets the current election term
func (e *Service) GetTerm(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	ch <- e.NewResult(proto.Marshal(&GetTermResponse{
		Term: e.getTerm(),
	}))
}

// Events registers the given channel to receive election events
func (e *Service) Events(bytes []byte, ch chan<- service.Result) {
	// Keep the stream open
}

func (e *Service) sendEvent(event *ListenResponse) {
	bytes, err := proto.Marshal(event)
	for _, session := range e.Sessions() {
		for _, ch := range session.ChannelsOf("Events") {
			ch <- e.NewResult(bytes, err)
		}
	}
}
