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
	mapapi "github.com/atomix/api/go/atomix/primitive/map"
	metaapi "github.com/atomix/api/go/atomix/primitive/meta"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/time"
	"sync"
)

func init() {
	registerService(func(protocol Protocol) Service {
		return &mapService{
			protocol: protocol,
			entries:  make(map[string]*mapapi.Entry),
		}
	})
}

type mapService struct {
	protocol Protocol
	entries  map[string]*mapapi.Entry
	streams  []chan<- mapapi.EventsResponse
	mu       sync.RWMutex
}

func (s *mapService) Protocol() Protocol {
	return s.protocol
}

func (s *mapService) Update(ctx context.Context, update *mapapi.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.entries[update.Key.Key]
	if !ok || meta.New(update.Key.ObjectMeta).After(meta.New(stored.Key.ObjectMeta)) {
		s.entries[update.Key.Key] = update
		return s.Protocol().Broadcast(ctx, update)
	}
	return nil
}

func (s *mapService) Read(ctx context.Context, key string) (*mapapi.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries[key], nil
}

func (s *mapService) List(ctx context.Context, ch chan<- mapapi.Entry) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, entry := range s.entries {
		ch <- *entry
	}
	return nil
}

func (s *mapService) Size(ctx context.Context, _ *mapapi.SizeRequest) (*mapapi.SizeResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &mapapi.SizeResponse{
		Size_: uint32(len(s.entries)),
	}, nil
}

func (s *mapService) Put(ctx context.Context, input *mapapi.PutRequest) (*mapapi.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[input.Entry.Key.Key]
	if ok {
		storedMeta := meta.New(entry.Key.ObjectMeta)
		updateMeta := meta.New(input.Entry.Key.ObjectMeta)
		if storedMeta.Timestamp.After(updateMeta.Timestamp) {
			return nil, errors.NewConflict("concurrent update")
		}
	}

	if err := checkPreconditions(entry, input.Preconditions); err != nil {
		return nil, err
	}

	entry = &input.Entry
	s.entries[input.Entry.Key.Key] = entry

	if !ok {
		s.notify(mapapi.Event{
			Type:  mapapi.Event_INSERT,
			Entry: *entry,
		})
	} else {
		s.notify(mapapi.Event{
			Type:  mapapi.Event_UPDATE,
			Entry: *entry,
		})
	}

	if err := s.Protocol().Broadcast(ctx, entry); err != nil {
		return nil, err
	}

	return &mapapi.PutResponse{
		Entry: *entry,
	}, nil
}

func (s *mapService) Get(ctx context.Context, input *mapapi.GetRequest) (*mapapi.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.entries[input.Key]
	if !ok {
		return nil, errors.NewNotFound("key '%s' not found", input.Key)
	}
	return &mapapi.GetResponse{
		Entry: *entry,
	}, nil
}

func (s *mapService) Remove(ctx context.Context, input *mapapi.RemoveRequest) (*mapapi.RemoveResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[input.Key.Key]
	if !ok {
		return nil, errors.NewNotFound("key '%s' not found", input.Key)
	}

	storedMeta := meta.New(entry.Key.ObjectMeta)
	updateMeta := meta.New(input.Key.ObjectMeta)
	if storedMeta.Timestamp.After(updateMeta.Timestamp) {
		return nil, errors.NewConflict("concurrent update")
	}

	if err := checkPreconditions(entry, input.Preconditions); err != nil {
		return nil, err
	}

	entry = &mapapi.Entry{
		Key: input.Key,
	}
	entry.Key.Type = metaapi.ObjectMeta_TOMBSTONE
	s.entries[input.Key.Key] = entry

	s.notify(mapapi.Event{
		Type:  mapapi.Event_REMOVE,
		Entry: *entry,
	})

	if err := s.Protocol().Broadcast(ctx, entry); err != nil {
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
			if err := s.Protocol().Broadcast(ctx, entry); err != nil {
				return nil, err
			}
		}
	}
	return &mapapi.ClearResponse{}, nil
}

func (s *mapService) Events(ctx context.Context, input *mapapi.EventsRequest, ch chan<- mapapi.EventsResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if input.Replay {
		for _, entry := range s.entries {
			ch <- mapapi.EventsResponse{
				Event: mapapi.Event{
					Type:  mapapi.Event_REPLAY,
					Entry: *entry,
				},
			}
		}
	}
	s.streams = append(s.streams, ch)
	return nil
}

func (s *mapService) Entries(ctx context.Context, input *mapapi.EntriesRequest, ch chan<- mapapi.EntriesResponse) error {
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
