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
	mapapi "github.com/atomix/atomix-api/go/atomix/primitive/map"
	metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(context ServiceContext) Service {
	return &mapService{
		ServiceContext: context,
		listeners:      make(map[ProposalID]*MapStateListener),
		entries:        make(map[string]*MapStateEntry),
		timers:         make(map[string]rsm.Timer),
	}
}

// mapService is a state machine for a map primitive
type mapService struct {
	ServiceContext
	listeners map[ProposalID]*MapStateListener
	entries   map[string]*MapStateEntry
	timers    map[string]rsm.Timer
}

func (m *mapService) notify(event *mapapi.EventsResponse) error {
	for proposalID, listener := range m.listeners {
		if listener.Key == "" || listener.Key == event.Event.Entry.Key.Key {
			proposal, ok := m.Proposals().Events().Get(proposalID)
			if ok {
				if err := proposal.Notify(event); err != nil {
					return err
				}
			} else {
				delete(m.listeners, proposalID)
			}
		}
	}
	return nil
}

func (m *mapService) GetState() (*MapState, error) {
	listeners := make([]MapStateListener, 0, len(m.listeners))
	for _, listener := range m.listeners {
		listeners = append(listeners, *listener)
	}
	entries := make([]MapStateEntry, 0, len(m.entries))
	for _, entry := range m.entries {
		entries = append(entries, *entry)
	}
	return &MapState{
		Listeners: listeners,
		Entries:   entries,
	}, nil
}

func (m *mapService) SetState(state *MapState) error {
	m.listeners = make(map[ProposalID]*MapStateListener)
	for _, state := range state.Listeners {
		listener := state
		m.listeners[listener.ProposalID] = &listener
	}
	m.entries = make(map[string]*MapStateEntry)
	for _, state := range state.Entries {
		entry := state
		m.entries[entry.Key.Key] = &entry
		m.scheduleTTL(entry.Key.Key, &entry)
	}
	return nil
}

func (m *mapService) Size(size SizeProposal) error {
	return size.Reply(&mapapi.SizeResponse{
		Size_: uint32(len(m.entries)),
	})
}

func (m *mapService) Put(put PutProposal) error {
	oldEntry := m.entries[put.Request().Entry.Key.Key]
	if err := checkPreconditions(oldEntry, put.Request().Preconditions); err != nil {
		return err
	}

	// If the value is equal to the current value, return a no-op.
	if oldEntry != nil && bytes.Equal(oldEntry.Value.Value, put.Request().Entry.Value.Value) {
		return put.Reply(&mapapi.PutResponse{
			Entry: *m.newEntry(oldEntry),
		})
	}

	// Create a new entry and increment the revision number
	newEntry := m.newEntryState(&put.Request().Entry)
	newEntry.Key.ObjectMeta.Revision = &metaapi.Revision{
		Num: metaapi.RevisionNum(put.ID()),
	}

	// Create a new entry value and set it in the map.
	m.entries[put.Request().Entry.Key.Key] = newEntry

	// Schedule the timeout for the value if necessary.
	m.scheduleTTL(put.Request().Entry.Key.Key, newEntry)

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
			Entry: *m.newEntry(newEntry),
		},
	})

	return put.Reply(&mapapi.PutResponse{
		Entry: *m.newEntry(newEntry),
	})
}

func (m *mapService) Get(get GetProposal) error {
	entry, ok := m.entries[get.Request().Key]
	if !ok {
		return errors.NewNotFound("key %s not found", get.Request().Key)
	}
	return get.Reply(&mapapi.GetResponse{
		Entry: *m.newEntry(entry),
	})
}

func (m *mapService) Remove(remove RemoveProposal) error {
	entry, ok := m.entries[remove.Request().Key.Key]
	if !ok {
		return errors.NewNotFound("key '%s' not found", remove.Request().Key.Key)
	}

	if err := checkPreconditions(entry, remove.Request().Preconditions); err != nil {
		return err
	}

	delete(m.entries, remove.Request().Key.Key)

	// Schedule the timeout for the value if necessary.
	m.cancelTTL(entry.Key.Key)

	// Publish an event to listener streams.
	m.notify(&mapapi.EventsResponse{
		Event: mapapi.Event{
			Type:  mapapi.Event_REMOVE,
			Entry: *m.newEntry(entry),
		},
	})

	return remove.Reply(&mapapi.RemoveResponse{
		Entry: *m.newEntry(entry),
	})
}

func (m *mapService) Clear(clear ClearProposal) error {
	for key, entry := range m.entries {
		m.notify(&mapapi.EventsResponse{
			Event: mapapi.Event{
				Type:  mapapi.Event_REMOVE,
				Entry: *m.newEntry(entry),
			},
		})
		m.cancelTTL(key)
		delete(m.entries, key)
	}
	return clear.Reply(&mapapi.ClearResponse{})
}

func (m *mapService) Events(events EventsProposal) error {
	m.listeners[events.ID()] = &MapStateListener{
		ProposalID: events.ID(),
		Key:        events.Request().Key,
	}
	if events.Request().Replay {
		for _, entry := range m.entries {
			if events.Request().Key == "" || events.Request().Key == entry.Key.Key {
				event := mapapi.Event{
					Type:  mapapi.Event_REPLAY,
					Entry: *m.newEntry(entry),
				}
				err := events.Notify(&mapapi.EventsResponse{
					Event: event,
				})
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (m *mapService) Entries(entries EntriesProposal) error {
	for _, entry := range m.entries {
		err := entries.Notify(&mapapi.EntriesResponse{
			Entry: *m.newEntry(entry),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *mapService) scheduleTTL(key string, entry *MapStateEntry) {
	m.cancelTTL(key)
	if entry.Value.Expire != nil {
		m.timers[key] = m.Scheduler().RunAt(*entry.Value.Expire, func() {
			delete(m.entries, key)
			m.notify(&mapapi.EventsResponse{
				Event: mapapi.Event{
					Type:  mapapi.Event_REMOVE,
					Entry: *m.newEntry(entry),
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

func (m *mapService) newEntryState(entry *mapapi.Entry) *MapStateEntry {
	state := &MapStateEntry{
		Key: MapStateKey{
			ObjectMeta: entry.Key.ObjectMeta,
			Key:        entry.Key.Key,
		},
	}
	if entry.Value != nil {
		state.Value = &MapStateValue{
			Value: entry.Value.Value,
		}
		if entry.Value.TTL != nil {
			expire := m.Scheduler().Time().Add(*entry.Value.TTL)
			state.Value.Expire = &expire
		}
	}
	return state
}

func (m *mapService) newEntry(state *MapStateEntry) *mapapi.Entry {
	entry := &mapapi.Entry{
		Key: mapapi.Key{
			ObjectMeta: state.Key.ObjectMeta,
			Key:        state.Key.Key,
		},
	}
	if state.Value != nil {
		entry.Value = &mapapi.Value{
			Value: state.Value.Value,
		}
		if state.Value.Expire != nil {
			ttl := m.Scheduler().Time().Sub(*state.Value.Expire)
			entry.Value.TTL = &ttl
		}
	}
	return entry
}

func checkPreconditions(entry *MapStateEntry, preconditions []mapapi.Precondition) error {
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
