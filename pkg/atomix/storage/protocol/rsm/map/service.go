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

package _map //nolint:golint

import (
	"bytes"
	mapapi "github.com/atomix/api/go/atomix/primitive/map"
	metaapi "github.com/atomix/api/go/atomix/primitive/meta"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &mapService{
		Service: rsm.NewService(scheduler, context),
		entries: make(map[string]*mapapi.Entry),
		timers:  make(map[string]rsm.Timer),
		streams: make(map[rsm.StreamID]ServiceEventsStream),
	}
}

// mapService is a state machine for a map primitive
type mapService struct {
	rsm.Service
	entries map[string]*mapapi.Entry
	timers  map[string]rsm.Timer
	streams map[rsm.StreamID]ServiceEventsStream
}

func (m *mapService) notify(event *mapapi.EventsResponse) error {
	for _, stream := range m.streams {
		if err := stream.Notify(event); err != nil {
			return err
		}
	}
	return nil
}

func (m *mapService) Size(*mapapi.SizeRequest) (*mapapi.SizeResponse, error) {
	return &mapapi.SizeResponse{
		Size_: uint32(len(m.entries)),
	}, nil
}

func (m *mapService) Put(request *mapapi.PutRequest) (*mapapi.PutResponse, error) {
	oldEntry := m.entries[request.Entry.Key.Key]
	if err := checkPreconditions(oldEntry, request.Preconditions); err != nil {
		return nil, err
	}

	// If the value is equal to the current value, return a no-op.
	if oldEntry != nil && bytes.Equal(oldEntry.Value.Value, request.Entry.Value.Value) {
		return &mapapi.PutResponse{
			Entry: *oldEntry,
		}, nil
	}

	// Create a new entry and increment the revision number
	newEntry := &request.Entry
	newEntry.Key.ObjectMeta.Revision = &metaapi.Revision{
		Num: metaapi.RevisionNum(m.Index()),
	}

	// Create a new entry value and set it in the map.
	m.entries[request.Entry.Key.Key] = newEntry

	// Schedule the timeout for the value if necessary.
	m.scheduleTTL(request.Entry.Key.Key, newEntry)

	// Publish an event to listener streams.
	var eventType mapapi.Event_Type
	if oldEntry != nil {
		eventType = mapapi.Event_UPDATE
	} else {
		eventType = mapapi.Event_INSERT
	}
	m.notify(&mapapi.EventsResponse{
		Event: mapapi.Event{
			Type:  eventType,
			Entry: *newEntry,
		},
	})

	return &mapapi.PutResponse{
		Entry: *newEntry,
	}, nil
}

func (m *mapService) Get(request *mapapi.GetRequest) (*mapapi.GetResponse, error) {
	entry, ok := m.entries[request.Key]
	if !ok {
		return nil, errors.NewNotFound("key %s not found", request.Key)
	}
	return &mapapi.GetResponse{
		Entry: *entry,
	}, nil
}

func (m *mapService) Remove(request *mapapi.RemoveRequest) (*mapapi.RemoveResponse, error) {
	entry, ok := m.entries[request.Key.Key]
	if !ok {
		return nil, errors.NewNotFound("key '%s' not found", request.Key.Key)
	}

	if err := checkPreconditions(entry, request.Preconditions); err != nil {
		return nil, err
	}

	delete(m.entries, request.Key.Key)

	// Schedule the timeout for the value if necessary.
	m.cancelTTL(entry.Key.Key)

	// Publish an event to listener streams.
	m.notify(&mapapi.EventsResponse{
		Event: mapapi.Event{
			Type:  mapapi.Event_REMOVE,
			Entry: *entry,
		},
	})

	return &mapapi.RemoveResponse{
		Entry: *entry,
	}, nil
}

func (m *mapService) Clear(*mapapi.ClearRequest) (*mapapi.ClearResponse, error) {
	for key, entry := range m.entries {
		m.notify(&mapapi.EventsResponse{
			Event: mapapi.Event{
				Type:  mapapi.Event_REMOVE,
				Entry: *entry,
			},
		})
		m.cancelTTL(key)
		delete(m.entries, key)
	}
	return &mapapi.ClearResponse{}, nil
}

func (m *mapService) Events(request *mapapi.EventsRequest, stream ServiceEventsStream) (rsm.StreamCloser, error) {
	m.streams[stream.ID()] = stream
	return func() {
		delete(m.streams, stream.ID())
	}, nil
}

func (m *mapService) Entries(request *mapapi.EntriesRequest, stream ServiceEntriesStream) (rsm.StreamCloser, error) {
	for _, entry := range m.entries {
		err := stream.Notify(&mapapi.EntriesResponse{
			Entry: *entry,
		})
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (m *mapService) scheduleTTL(key string, entry *mapapi.Entry) {
	m.cancelTTL(key)
	if entry.Value.TTL != nil && *entry.Value.TTL > 0 {
		m.timers[key] = m.ScheduleOnce(*entry.Value.TTL, func() {
			delete(m.entries, key)
			m.notify(&mapapi.EventsResponse{
				Event: mapapi.Event{
					Type:  mapapi.Event_REMOVE,
					Entry: *entry,
				},
			})
		})
	}
}

func (m *mapService) cancelTTL(key string) {
	timer, ok := m.timers[key]
	if ok {
		timer.Cancel()
	}
}

func checkPreconditions(entry *mapapi.Entry, preconditions []mapapi.Precondition) error {
	for _, precondition := range preconditions {
		switch p := precondition.Precondition.(type) {
		case *mapapi.Precondition_Metadata:
			if p.Metadata.Type == metaapi.ObjectMeta_TOMBSTONE {
				if entry != nil {
					return errors.NewConflict("metadata precondition failed")
				}
			} else {
				if entry == nil {
					return errors.NewConflict("metadata precondition failed")
				}
				if !meta.Equal(entry.Key.ObjectMeta, *p.Metadata) {
					return errors.NewConflict("metadata precondition failed")
				}
			}
		}
	}
	return nil
}
