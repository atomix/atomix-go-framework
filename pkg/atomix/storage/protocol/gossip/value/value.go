// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	valueapi "github.com/atomix/atomix-api/go/atomix/primitive/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/time"
	"sync"
)

func init() {
	registerService(func(protocol GossipProtocol) (Service, error) {
		service := &valueService{
			protocol: protocol,
			value: &valueapi.Value{
				ObjectMeta: meta.NewTimestamped(protocol.Clock().Get()).AsTombstone().Proto(),
			},
		}
		if err := protocol.Server().Register(&valueHandler{service}); err != nil {
			return nil, err
		}
		return service, nil
	})
}

type valueHandler struct {
	service *valueService
}

func (s *valueHandler) Read(ctx context.Context) (*ValueState, error) {
	s.service.mu.RLock()
	defer s.service.mu.RUnlock()
	value := s.service.value
	return &ValueState{
		ObjectMeta: value.ObjectMeta,
		Value:      value.Value,
	}, nil
}

func (s *valueHandler) Update(ctx context.Context, value *ValueState) error {
	s.service.mu.Lock()
	defer s.service.mu.Unlock()
	if meta.FromProto(value.ObjectMeta).After(meta.FromProto(s.service.value.ObjectMeta)) {
		s.service.value = &valueapi.Value{
			ObjectMeta: value.ObjectMeta,
			Value:      value.Value,
		}
	}
	return nil
}

var _ GossipHandler = &valueHandler{}

type valueService struct {
	protocol GossipProtocol
	value    *valueapi.Value
	streams  []chan<- valueapi.EventsResponse
	mu       sync.RWMutex
}

func (s *valueService) Protocol() GossipProtocol {
	return s.protocol
}

func (s *valueService) Set(ctx context.Context, request *valueapi.SetRequest) (*valueapi.SetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !time.NewTimestamp(*request.Headers.Timestamp).After(time.NewTimestamp(*s.value.Timestamp)) {
		return &valueapi.SetResponse{
			Value: *s.value,
		}, nil
	}

	err := checkPreconditions(s.value, request.Preconditions)
	if err != nil {
		return nil, err
	}

	s.value = &request.Value
	s.value.Timestamp = request.Headers.Timestamp

	err = s.protocol.Group().Update(ctx, &ValueState{
		ObjectMeta: s.value.ObjectMeta,
		Value:      s.value.Value,
	})
	if err != nil {
		return nil, err
	}

	s.notify(valueapi.Event{
		Type:  valueapi.Event_UPDATE,
		Value: request.Value,
	})
	return &valueapi.SetResponse{
		Value: request.Value,
	}, nil
}

func (s *valueService) Get(ctx context.Context, request *valueapi.GetRequest) (*valueapi.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var value *valueapi.Value
	if s.value != nil {
		value = s.value
	}
	state, err := s.protocol.Group().Repair(ctx, &ValueState{
		ObjectMeta: value.ObjectMeta,
		Value:      value.Value,
	})
	if err != nil {
		return nil, err
	}
	s.value = &valueapi.Value{
		ObjectMeta: state.ObjectMeta,
		Value:      state.Value,
	}
	return &valueapi.GetResponse{
		Value: *s.value,
	}, nil
}

func (s *valueService) Events(ctx context.Context, request *valueapi.EventsRequest, ch chan<- valueapi.EventsResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streams = append(s.streams, ch)
	ch <- valueapi.EventsResponse{}
	return nil
}

func (s *valueService) notify(event valueapi.Event) {
	for _, ch := range s.streams {
		ch <- valueapi.EventsResponse{
			Event: event,
		}
	}
}

func checkPreconditions(value *valueapi.Value, preconditions []valueapi.Precondition) error {
	for _, precondition := range preconditions {
		switch p := precondition.Precondition.(type) {
		case *valueapi.Precondition_Metadata:
			if value == nil {
				return errors.NewConflict("metadata precondition failed")
			}
			if !meta.FromProto(value.ObjectMeta).Equal(meta.FromProto(*p.Metadata)) {
				return errors.NewConflict("metadata mismatch")
			}
		}
	}
	return nil
}
