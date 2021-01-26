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

package counter

import (
	"context"
	"github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/time"
	"sync"
)

func init() {
	registerService(func(protocol GossipProtocol) (Service, error) {
		service := &counterService{
			protocol: protocol,
			state: &CounterState{
				ObjectMeta: meta.NewTimestamped(protocol.Clock().Get()).Proto(),
				Increments: make(map[string]int64),
				Decrements: make(map[string]int64),
			},
		}
		if err := protocol.Server().Register(&counterHandler{service}); err != nil {
			return nil, err
		}
		return service, nil
	})
}

type counterHandler struct {
	service *counterService
}

func (s *counterHandler) Read(ctx context.Context) (*CounterState, error) {
	s.service.mu.RLock()
	defer s.service.mu.RUnlock()
	return s.service.state, nil
}

func (s *counterHandler) Update(ctx context.Context, state *CounterState) error {
	s.service.mu.Lock()
	defer s.service.mu.Unlock()
	if meta.FromProto(state.ObjectMeta).Equal(meta.FromProto(s.service.state.ObjectMeta)) {
		for memberID, delta := range state.Increments {
			if delta > s.service.state.Increments[memberID] {
				s.service.state.Increments[memberID] = delta
			}
		}
		for memberID, delta := range state.Decrements {
			if delta > s.service.state.Decrements[memberID] {
				s.service.state.Decrements[memberID] = delta
			}
		}
	} else if meta.FromProto(state.ObjectMeta).After(meta.FromProto(s.service.state.ObjectMeta)) {
		s.service.state = state
	}
	return nil
}

var _ GossipHandler = &counterHandler{}

type counterService struct {
	protocol GossipProtocol
	state    *CounterState
	mu       sync.RWMutex
}

func (s *counterService) getValue() int64 {
	var value int64
	for _, delta := range s.state.Increments {
		value += delta
	}
	for _, delta := range s.state.Decrements {
		value -= delta
	}
	return value
}

func (s *counterService) Set(ctx context.Context, request *counter.SetRequest) (*counter.SetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.NewTimestamp(*request.Headers.Timestamp).After(time.NewTimestamp(*s.state.Timestamp)) {
		s.state = &CounterState{
			ObjectMeta: meta.NewTimestamped(time.NewTimestamp(*request.Headers.Timestamp)).Proto(),
			Increments: make(map[string]int64),
			Decrements: make(map[string]int64),
		}
		if request.Value > 0 {
			s.state.Increments[s.protocol.Group().MemberID().String()] = request.Value
		} else {
			s.state.Decrements[s.protocol.Group().MemberID().String()] = request.Value * -1
		}
		if err := s.protocol.Group().Update(ctx, s.state); err != nil {
			return nil, err
		}
	}
	return &counter.SetResponse{
		Value: s.getValue(),
	}, nil
}

func (s *counterService) Get(ctx context.Context, request *counter.GetRequest) (*counter.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var value int64
	for _, delta := range s.state.Increments {
		value += delta
	}
	for _, delta := range s.state.Decrements {
		value -= delta
	}
	return &counter.GetResponse{
		Value: value,
	}, nil
}

func (s *counterService) Increment(ctx context.Context, request *counter.IncrementRequest) (*counter.IncrementResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if request.Delta > 0 {
		s.state.Increments[s.protocol.Group().MemberID().String()] = s.state.Increments[s.protocol.Group().MemberID().String()] + request.Delta
	} else {
		s.state.Decrements[s.protocol.Group().MemberID().String()] = s.state.Decrements[s.protocol.Group().MemberID().String()] + request.Delta
	}
	if err := s.protocol.Group().Update(ctx, s.state); err != nil {
		return nil, err
	}
	return &counter.IncrementResponse{
		Value: s.getValue(),
	}, nil
}

func (s *counterService) Decrement(ctx context.Context, request *counter.DecrementRequest) (*counter.DecrementResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if request.Delta > 0 {
		s.state.Decrements[s.protocol.Group().MemberID().String()] = s.state.Decrements[s.protocol.Group().MemberID().String()] + request.Delta
	} else {
		s.state.Increments[s.protocol.Group().MemberID().String()] = s.state.Increments[s.protocol.Group().MemberID().String()] + request.Delta
	}
	if err := s.protocol.Group().Update(ctx, s.state); err != nil {
		return nil, err
	}
	return &counter.DecrementResponse{
		Value: s.getValue(),
	}, nil
}

var _ Service = &counterService{}
