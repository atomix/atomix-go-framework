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

package _map

import (
	"context"
	mapapi "github.com/atomix/atomix-api/go/atomix/primitive/map"
	metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/time"
	"sync"
)

func init() {
	registerService(func(protocol GossipProtocol) (Service, error) {
		service := &mapService{
			protocol: protocol,
			entries:  make(map[string]*mapapi.Entry),
		}
		if err := protocol.Server().Register(&mapHandler{service}); err != nil {
			return nil, err
		}
		return service, nil
	})
}

type mapHandler struct {
	service *mapService
}

func (s *mapHandler) Update(ctx context.Context, update *MapEntry) error {
	s.service.mu.Lock()
	defer s.service.mu.Unlock()
	stored, ok := s.service.entries[update.Key.Key]
	if !ok || meta.FromProto(update.Key.ObjectMeta).After(meta.FromProto(stored.Key.ObjectMeta)) {
		s.service.entries[update.Key.Key] = newMapEntry(update)
		return s.service.protocol.Group().Update(ctx, update)
	}
	return nil
}

func (s *mapHandler) Read(ctx context.Context, key string) (*MapEntry, error) {
	s.service.mu.RLock()
	defer s.service.mu.RUnlock()
	return newStateEntry(s.service.entries[key]), nil
}

func (s *mapHandler) List(ctx context.Context, ch chan<- MapEntry) error {
	s.service.mu.RLock()
	defer s.service.mu.RUnlock()
	for _, entry := range s.service.entries {
		ch <- *newStateEntry(entry)
	}
	return nil
}

var _ GossipHandler = &mapHandler{}

func newStateEntry(entry *mapapi.Entry) *MapEntry {
	if entry == nil {
		return nil
	}
	state := &MapEntry{
		Key: MapKey{
			ObjectMeta: entry.Key.ObjectMeta,
			Key:        entry.Key.Key,
		},
	}
	if entry.Value != nil {
		state.Value = &MapValue{
			Value: entry.Value.Value,
			TTL:   entry.Value.TTL,
		}
	}
	return state
}

func newMapEntry(state *MapEntry) *mapapi.Entry {
	if state == nil {
		return nil
	}
	entry := &mapapi.Entry{
		Key: mapapi.Key{
			ObjectMeta: state.Key.ObjectMeta,
			Key:        state.Key.Key,
		},
	}
	if state.Value != nil {
		entry.Value = &mapapi.Value{
			Value: state.Value.Value,
			TTL:   state.Value.TTL,
		}
	}
	return entry
}

type mapService struct {
	protocol GossipProtocol
	entries  map[string]*mapapi.Entry
	streams  []chan<- mapapi.EventsResponse
	mu       sync.RWMutex
}

func (s *mapService) Size(ctx context.Context, _ *mapapi.SizeRequest) (*mapapi.SizeResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &mapapi.SizeResponse{
		Size_: uint32(len(s.entries)),
	}, nil
}

func (s *mapService) Put(ctx context.Context, request *mapapi.PutRequest) (*mapapi.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldEntry, ok := s.entries[request.Entry.Key.Key]
	if !ok {
		newEntry := &request.Entry
		newEntry.Key.Timestamp = request.Headers.Timestamp
		s.entries[request.Entry.Key.Key] = newEntry
		if err := s.protocol.Group().Update(ctx, newStateEntry(newEntry)); err != nil {
			return nil, err
		}
		s.notify(mapapi.Event{
			Type:  mapapi.Event_INSERT,
			Entry: *newEntry,
		})
		return &mapapi.PutResponse{
			Entry: *newEntry,
		}, nil
	}

	if err := checkPreconditions(oldEntry, request.Preconditions); err != nil {
		return nil, err
	}

	if oldEntry.Key.Timestamp != nil && time.NewTimestamp(*oldEntry.Key.Timestamp).After(time.NewTimestamp(*request.Headers.Timestamp)) {
		return &mapapi.PutResponse{
			Entry: *oldEntry,
		}, nil
	}

	newEntry := &request.Entry
	newEntry.Key.Timestamp = request.Headers.Timestamp
	s.entries[request.Entry.Key.Key] = newEntry

	if err := s.protocol.Group().Update(ctx, newStateEntry(newEntry)); err != nil {
		return nil, err
	}

	if !ok || oldEntry.Key.Type == metaapi.ObjectMeta_TOMBSTONE {
		s.notify(mapapi.Event{
			Type:  mapapi.Event_INSERT,
			Entry: *newEntry,
		})
	} else {
		s.notify(mapapi.Event{
			Type:  mapapi.Event_UPDATE,
			Entry: *newEntry,
		})
	}

	return &mapapi.PutResponse{
		Entry: *newEntry,
	}, nil
}

func (s *mapService) Get(ctx context.Context, request *mapapi.GetRequest) (*mapapi.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.entries[request.Key]
	if !ok {
		return nil, errors.NewNotFound("key '%s' not found", request.Key)
	}
	state, err := s.protocol.Group().Repair(ctx, newStateEntry(entry))
	if err != nil {
		return nil, err
	}
	return &mapapi.GetResponse{
		Entry: *newMapEntry(state),
	}, nil
}

func (s *mapService) Remove(ctx context.Context, request *mapapi.RemoveRequest) (*mapapi.RemoveResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[request.Key.Key]
	if !ok {
		return nil, errors.NewNotFound("key '%s' not found", request.Key)
	}

	storedMeta := meta.FromProto(entry.Key.ObjectMeta)
	updateMeta := meta.FromProto(request.Key.ObjectMeta)
	if storedMeta.Timestamp.After(updateMeta.Timestamp) {
		return nil, errors.NewConflict("concurrent update")
	}

	if err := checkPreconditions(entry, request.Preconditions); err != nil {
		return nil, err
	}

	entry = &mapapi.Entry{
		Key: request.Key,
	}
	entry.Key.Type = metaapi.ObjectMeta_TOMBSTONE
	s.entries[request.Key.Key] = entry

	s.notify(mapapi.Event{
		Type:  mapapi.Event_REMOVE,
		Entry: *entry,
	})

	if err := s.protocol.Group().Update(ctx, newStateEntry(entry)); err != nil {
		return nil, err
	}

	return &mapapi.RemoveResponse{
		Entry: *entry,
	}, nil
}

func (s *mapService) Clear(ctx context.Context, _ *mapapi.ClearRequest) (*mapapi.ClearResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.entries {
		if entry.Key.Type != metaapi.ObjectMeta_TOMBSTONE {
			entry.Key.Type = metaapi.ObjectMeta_TOMBSTONE
			if err := s.protocol.Group().Update(ctx, newStateEntry(entry)); err != nil {
				return nil, err
			}
		}
	}
	return &mapapi.ClearResponse{}, nil
}

func (s *mapService) Events(ctx context.Context, request *mapapi.EventsRequest, ch chan<- mapapi.EventsResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streams = append(s.streams, ch)
	ch <- mapapi.EventsResponse{}
	if request.Replay {
		for _, entry := range s.entries {
			ch <- mapapi.EventsResponse{
				Event: mapapi.Event{
					Type:  mapapi.Event_REPLAY,
					Entry: *entry,
				},
			}
		}
	}
	return nil
}

func (s *mapService) Entries(ctx context.Context, request *mapapi.EntriesRequest, ch chan<- mapapi.EntriesResponse) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, entry := range s.entries {
		ch <- mapapi.EntriesResponse{
			Entry: *entry,
		}
	}
	return nil
}

func (s *mapService) notify(event mapapi.Event) {
	for _, ch := range s.streams {
		ch <- mapapi.EventsResponse{
			Event: event,
		}
	}
}

func checkPreconditions(entry *mapapi.Entry, preconditions []mapapi.Precondition) error {
	for _, precondition := range preconditions {
		switch p := precondition.Precondition.(type) {
		case *mapapi.Precondition_Metadata:
			if entry == nil {
				return errors.NewConflict("metadata precondition failed")
			}
			if !time.NewTimestamp(*entry.Key.ObjectMeta.Timestamp).Equal(time.NewTimestamp(*p.Metadata.Timestamp)) {
				return errors.NewConflict("metadata mismatch")
			}
		}
	}
	return nil
}
