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

// RegisterElectionService registers the election service in the given service registry
func RegisterElectionService(registry *service.ServiceRegistry) {
	registry.Register("election", newElectionService)
}

// newElectionService returns a new ElectionService
func newElectionService(context service.Context) service.Service {
	service := &ElectionService{
		SessionizedService: service.NewSessionizedService(context),
		candidates:         make([]*ElectionRegistration, 0),
	}
	service.init()
	return service
}

// ElectionService is a state machine for an election primitive
type ElectionService struct {
	*service.SessionizedService
	leader     *ElectionRegistration
	term       uint64
	timestamp  *time.Time
	candidates []*ElectionRegistration
}

// init initializes the election service
func (e *ElectionService) init() {
	e.Executor.Register("Enter", e.Enter)
	e.Executor.Register("Withdraw", e.Withdraw)
	e.Executor.Register("Anoint", e.Anoint)
	e.Executor.Register("Promote", e.Promote)
	e.Executor.Register("Evict", e.Evict)
	e.Executor.Register("GetLeadership", e.GetLeadership)
	e.Executor.Register("Events", e.Events)
}

// Backup backs up the list service
func (e *ElectionService) Backup() ([]byte, error) {
	snapshot := &ElectionSnapshot{
		Term:       e.term,
		Timestamp:  e.timestamp,
		Leader:     e.leader,
		Candidates: e.candidates,
	}
	return proto.Marshal(snapshot)
}

// Restore restores the list service
func (e *ElectionService) Restore(bytes []byte) error {
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

func (e *ElectionService) getCandidates() []string {
	candidates := make([]string, len(e.candidates))
	for i, candidate := range e.candidates {
		candidates[i] = candidate.ID
	}
	return candidates
}

func (e *ElectionService) Enter(bytes []byte, ch chan<- service.Result) {
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
		Type:       ListenResponse_CHANGED,
		Term:       e.term,
		Timestamp:  e.timestamp,
		Leader:     e.leader.ID,
		Candidates: e.getCandidates(),
	})

	ch <- e.NewResult(proto.Marshal(&EnterResponse{
		Term:       e.term,
		Timestamp:  e.timestamp,
		Leader:     e.leader.ID,
		Candidates: []string{e.leader.ID},
	}))
}

func (e *ElectionService) Withdraw(bytes []byte, ch chan<- service.Result) {
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
			Type:       ListenResponse_CHANGED,
			Term:       e.term,
			Timestamp:  e.timestamp,
			Leader:     e.leader.ID,
			Candidates: e.getCandidates(),
		})

		ch <- e.NewResult(proto.Marshal(&WithdrawResponse{
			Succeeded: true,
		}))
	} else {
		ch <- e.NewResult(proto.Marshal(&WithdrawResponse{
			Succeeded: false,
		}))
	}
}

func (e *ElectionService) Anoint(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &AnointRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- e.NewFailure(err)
		return
	}

	if e.leader != nil && e.leader.ID == request.ID {
		ch <- e.NewResult(proto.Marshal(&AnointResponse{
			Succeeded: true,
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
			Succeeded: false,
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
		Type:       ListenResponse_CHANGED,
		Term:       e.term,
		Timestamp:  e.timestamp,
		Leader:     e.leader.ID,
		Candidates: e.getCandidates(),
	})

	ch <- e.NewResult(proto.Marshal(&AnointResponse{
		Succeeded: true,
	}))
}

func (e *ElectionService) Promote(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &PromoteRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- e.NewFailure(err)
		return
	}

	if e.leader != nil && e.leader.ID == request.ID {
		ch <- e.NewResult(proto.Marshal(&PromoteResponse{
			Succeeded: true,
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
			Succeeded: false,
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
		Type:       ListenResponse_CHANGED,
		Term:       e.term,
		Timestamp:  e.timestamp,
		Leader:     e.leader.ID,
		Candidates: e.getCandidates(),
	})

	ch <- e.NewResult(proto.Marshal(&AnointResponse{
		Succeeded: true,
	}))
}

func (e *ElectionService) Evict(bytes []byte, ch chan<- service.Result) {
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
			Type:       ListenResponse_CHANGED,
			Term:       e.term,
			Timestamp:  e.timestamp,
			Leader:     e.leader.ID,
			Candidates: e.getCandidates(),
		})

		ch <- e.NewResult(proto.Marshal(&WithdrawResponse{
			Succeeded: true,
		}))
	} else {
		ch <- e.NewResult(proto.Marshal(&WithdrawResponse{
			Succeeded: false,
		}))
	}
}

func (e *ElectionService) GetLeadership(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	ch <- e.NewResult(proto.Marshal(&GetLeadershipResponse{
		Term:       e.term,
		Timestamp:  e.timestamp,
		Leader:     e.leader.ID,
		Candidates: e.getCandidates(),
	}))
}

func (e *ElectionService) Events(bytes []byte, ch chan<- service.Result) {
	// Keep the stream open
}

func (e *ElectionService) sendEvent(event *ListenResponse) {
	bytes, err := proto.Marshal(event)
	for _, session := range e.Sessions() {
		for _, ch := range session.ChannelsOf("Events") {
			ch <- e.NewResult(bytes, err)
		}
	}
}
