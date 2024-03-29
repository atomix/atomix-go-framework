// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"context"
	"github.com/atomix/atomix-api/go/atomix/primitive/counter"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/time"
	"sync"
)

func init() {
	registerService(func(protocol GossipProtocol) (Service, error) {
		service := &counterService{
			protocol: protocol,
			state: &CounterState{
				ObjectMeta: meta.NewTimestamped(protocol.Clock().Scheme().NewClock().Get()).Proto(),
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
		log.Debugf("Update %s", s.service.protocol.Group().MemberID())
		for memberID, delta := range state.Increments {
			if delta > s.service.state.Increments[memberID] {
				log.Debugf("Update %s Increments[%s] = %d", s.service.protocol.Group().MemberID(), memberID, delta)
				s.service.state.Increments[memberID] = delta
			}
		}
		for memberID, delta := range state.Decrements {
			if delta > s.service.state.Decrements[memberID] {
				log.Debugf("Update %s Decrements[%s] = %d", s.service.protocol.Group().MemberID(), memberID, delta)
				s.service.state.Decrements[memberID] = delta
			}
		}
	} else if meta.FromProto(state.ObjectMeta).After(meta.FromProto(s.service.state.ObjectMeta)) {
		log.Debugf("Update %s state=%+v", s.service.protocol.Group().MemberID(), state)
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

func (s *counterService) Protocol() GossipProtocol {
	return s.protocol
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
	log.Infof("Decrement %s", s.protocol.Group().MemberID())
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
