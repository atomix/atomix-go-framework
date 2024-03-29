// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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

func (m *mapService) notify(event *mapapi.EventsResponse) {
	for proposalID, listener := range m.listeners {
		if listener.Key == "" || listener.Key == event.Event.Entry.Key.Key {
			proposal, ok := m.Proposals().Events().Get(proposalID)
			if ok {
				proposal.Notify(event)
			} else {
				delete(m.listeners, proposalID)
			}
		}
	}
}

func (m *mapService) Backup(writer SnapshotWriter) error {
	listeners := make([]MapStateListener, 0, len(m.listeners))
	for _, listener := range m.listeners {
		listeners = append(listeners, *listener)
	}
	entries := make([]MapStateEntry, 0, len(m.entries))
	for _, entry := range m.entries {
		entries = append(entries, *entry)
	}
	return writer.WriteState(&MapState{
		Listeners: listeners,
		Entries:   entries,
	})
}

func (m *mapService) Restore(reader SnapshotReader) error {
	state, err := reader.ReadState()
	if err != nil {
		return err
	}
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

func (m *mapService) Size(SizeQuery) (*mapapi.SizeResponse, error) {
	return &mapapi.SizeResponse{
		Size_: uint32(len(m.entries)),
	}, nil
}

func (m *mapService) Put(put PutProposal) (*mapapi.PutResponse, error) {
	oldEntry := m.entries[put.Request().Entry.Key.Key]
	if err := checkPreconditions(oldEntry, put.Request().Preconditions); err != nil {
		return nil, err
	}

	// If the value is equal to the current value, return a no-op.
	if oldEntry != nil && bytes.Equal(oldEntry.Value.Value, put.Request().Entry.Value.Value) {
		return &mapapi.PutResponse{
			Entry: *m.newEntry(oldEntry),
		}, nil
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

	return &mapapi.PutResponse{
		Entry: *m.newEntry(newEntry),
	}, nil
}

func (m *mapService) Get(get GetQuery) (*mapapi.GetResponse, error) {
	entry, ok := m.entries[get.Request().Key]
	if !ok {
		return nil, errors.NewNotFound("key %s not found", get.Request().Key)
	}
	return &mapapi.GetResponse{
		Entry: *m.newEntry(entry),
	}, nil
}

func (m *mapService) Remove(remove RemoveProposal) (*mapapi.RemoveResponse, error) {
	entry, ok := m.entries[remove.Request().Key.Key]
	if !ok {
		return nil, errors.NewNotFound("key '%s' not found", remove.Request().Key.Key)
	}

	if err := checkPreconditions(entry, remove.Request().Preconditions); err != nil {
		return nil, err
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

	return &mapapi.RemoveResponse{
		Entry: *m.newEntry(entry),
	}, nil
}

func (m *mapService) Clear(ClearProposal) (*mapapi.ClearResponse, error) {
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
	return &mapapi.ClearResponse{}, nil
}

func (m *mapService) Events(events EventsProposal) {
	m.listeners[events.ID()] = &MapStateListener{
		ProposalID: events.ID(),
		Key:        events.Request().Key,
	}

	events.Notify(&mapapi.EventsResponse{})

	if events.Request().Replay {
		for _, entry := range m.entries {
			if events.Request().Key == "" || events.Request().Key == entry.Key.Key {
				event := mapapi.Event{
					Type:  mapapi.Event_REPLAY,
					Entry: *m.newEntry(entry),
				}
				events.Notify(&mapapi.EventsResponse{
					Event: event,
				})
			}
		}
	}
}

func (m *mapService) Entries(entries EntriesQuery) {
	defer entries.Close()
	for _, entry := range m.entries {
		entries.Notify(&mapapi.EntriesResponse{
			Entry: *m.newEntry(entry),
		})
	}
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
					return errors.NewAlreadyExists("metadata precondition failed")
				}
			} else {
				if entry == nil {
					return errors.NewNotFound("metadata precondition failed")
				}
				if !meta.Equal(entry.Key.ObjectMeta, *p.Metadata) {
					return errors.NewConflict("metadata precondition failed")
				}
			}
		}
	}
	return nil
}
