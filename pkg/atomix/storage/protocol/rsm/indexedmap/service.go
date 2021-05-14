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

package indexedmap

import (
	"bytes"
	indexedmapapi "github.com/atomix/atomix-api/go/atomix/primitive/indexedmap"
	metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(context ServiceContext) Service {
	return &indexedMapService{
		ServiceContext: context,
		entries:        make(map[string]*LinkedMapEntryValue),
		indexes:        make(map[uint64]*LinkedMapEntryValue),
		timers:         make(map[string]rsm.Timer),
	}
}

// indexedMapService is a state machine for a map primitive
type indexedMapService struct {
	ServiceContext
	lastIndex  uint64
	entries    map[string]*LinkedMapEntryValue
	indexes    map[uint64]*LinkedMapEntryValue
	firstEntry *LinkedMapEntryValue
	lastEntry  *LinkedMapEntryValue
	listeners  map[ProposalID]*IndexedMapStateListener
	timers     map[string]rsm.Timer
}

func (m *indexedMapService) Backup(writer SnapshotWriter) error {
	return writer.WriteState(&IndexedMapState{})
}

func (m *indexedMapService) Restore(reader SnapshotReader) error {
	_, err := reader.ReadState()
	if err != nil {
		return err
	}
	return nil
}

func (m *indexedMapService) SetState(state *IndexedMapState) error {
	m.listeners = make(map[ProposalID]*IndexedMapStateListener)
	for _, state := range state.Listeners {
		listener := state
		m.listeners[listener.ProposalID] = &listener
	}

	var prevEntry *LinkedMapEntryValue
	m.firstEntry = nil
	m.lastEntry = nil
	m.entries = make(map[string]*LinkedMapEntryValue)
	m.indexes = make(map[uint64]*LinkedMapEntryValue)
	for _, state := range state.Entries {
		entry := state
		linkedEntry := &LinkedMapEntryValue{
			IndexedMapEntry: &entry,
		}
		if m.firstEntry == nil {
			m.firstEntry = linkedEntry
		}
		m.entries[linkedEntry.Key] = linkedEntry
		m.indexes[linkedEntry.Index] = linkedEntry
		if prevEntry != nil {
			prevEntry.Next = linkedEntry
			linkedEntry.Prev = prevEntry
		}
		prevEntry = linkedEntry
		m.lastEntry = linkedEntry
		m.scheduleTTL(entry.Key, &entry)
	}
	return nil
}

func (m *indexedMapService) GetState() (*IndexedMapState, error) {
	listeners := make([]IndexedMapStateListener, 0, len(m.listeners))
	for _, listener := range m.listeners {
		listeners = append(listeners, *listener)
	}
	entries := make([]IndexedMapEntry, 0, len(m.entries))
	entry := m.firstEntry
	for entry != nil {
		entries = append(entries, *entry.IndexedMapEntry)
		entry = entry.Next
	}
	return &IndexedMapState{
		Listeners: listeners,
		Entries:   entries,
	}, nil
}

func (m *indexedMapService) notify(event *indexedmapapi.EventsResponse) error {
	for proposalID, listener := range m.listeners {
		if listener.Key == "" || listener.Key == event.Event.Entry.Key {
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

func (m *indexedMapService) Size(size SizeProposal) error {
	return size.Reply(&indexedmapapi.SizeResponse{
		Size_: uint32(len(m.entries)),
	})
}

func (m *indexedMapService) Put(put PutProposal) error {
	var oldEntry *LinkedMapEntryValue
	if put.Request().Entry.Index > 0 {
		oldEntry = m.indexes[put.Request().Entry.Index]
		if oldEntry != nil && oldEntry.Key != put.Request().Entry.Key {
			return errors.NewConflict("indexes do not match")
		}
	} else {
		oldEntry = m.entries[put.Request().Entry.Key]
	}

	var entry *IndexedMapEntry
	if oldEntry != nil {
		entry = oldEntry.IndexedMapEntry
	}
	if err := checkPreconditions(entry, put.Request().Preconditions); err != nil {
		return err
	}

	if oldEntry == nil {
		// Increment the index for a new entry
		var index uint64
		if put.Request().Entry.Index > 0 {
			if put.Request().Entry.Index > m.lastIndex {
				m.lastIndex = put.Request().Entry.Index
			}
			index = put.Request().Entry.Index
		} else {
			m.lastIndex++
			index = m.lastIndex
		}

		// Create a new entry value and set it in the map.
		newEntry := &LinkedMapEntryValue{
			IndexedMapEntry: &IndexedMapEntry{
				IndexedMapEntryPosition: IndexedMapEntryPosition{
					Index: index,
					Key:   put.Request().Entry.Key,
				},
				IndexedMapEntryValue: IndexedMapEntryValue{
					Value: put.Request().Entry.Value.Value,
				},
			},
		}
		if put.Request().Entry.TTL != nil {
			expire := m.Scheduler().Time().Add(*put.Request().Entry.TTL)
			newEntry.Expire = &expire
		}
		m.entries[newEntry.Key] = newEntry
		m.indexes[newEntry.Index] = newEntry

		// Set the first entry if not set
		if m.firstEntry == nil {
			m.firstEntry = newEntry
		}

		// If the last entry is set, link it to the new entry
		if put.Request().Entry.Index > 0 {
			if m.lastIndex == put.Request().Entry.Index {
				if m.lastEntry != nil {
					m.lastEntry.Next = newEntry
					newEntry.Prev = m.lastEntry
				}
				m.lastEntry = newEntry
			}
		} else {
			if m.lastEntry != nil {
				m.lastEntry.Next = newEntry
				newEntry.Prev = m.lastEntry
			}
		}

		// Update the last entry
		m.lastEntry = newEntry

		// Schedule the timeout for the value if necessary.
		m.scheduleTTL(put.Request().Entry.Key, newEntry.IndexedMapEntry)

		m.notify(&indexedmapapi.EventsResponse{
			Event: indexedmapapi.Event{
				Type:  indexedmapapi.Event_INSERT,
				Entry: *m.newEntry(newEntry.IndexedMapEntry),
			},
		})

		return put.Reply(&indexedmapapi.PutResponse{
			Entry: m.newEntry(newEntry.IndexedMapEntry),
		})
	}

	// If the value is equal to the current value, return a no-op.
	if bytes.Equal(oldEntry.Value, put.Request().Entry.Value.Value) {
		return put.Reply(&indexedmapapi.PutResponse{
			Entry: m.newEntry(oldEntry.IndexedMapEntry),
		})
	}

	// Create a new entry value and set it in the map.
	newEntry := &LinkedMapEntryValue{
		IndexedMapEntry: &IndexedMapEntry{
			IndexedMapEntryPosition: IndexedMapEntryPosition{
				Index: oldEntry.Index,
				Key:   oldEntry.Key,
			},
			IndexedMapEntryValue: IndexedMapEntryValue{
				Value: put.Request().Entry.Value.Value,
			},
		},
		Prev: oldEntry.Prev,
		Next: oldEntry.Next,
	}
	if put.Request().Entry.TTL != nil {
		expire := m.Scheduler().Time().Add(*put.Request().Entry.TTL)
		newEntry.Expire = &expire
	}
	m.entries[newEntry.Key] = newEntry
	m.indexes[newEntry.Index] = newEntry

	// Update links for previous and next entries
	if newEntry.Prev != nil {
		oldEntry.Prev.Next = newEntry
	} else {
		m.firstEntry = newEntry
	}
	if newEntry.Next != nil {
		oldEntry.Next.Prev = newEntry
	} else {
		m.lastEntry = newEntry
	}

	// Schedule the timeout for the value if necessary.
	m.scheduleTTL(newEntry.Key, newEntry.IndexedMapEntry)

	m.notify(&indexedmapapi.EventsResponse{
		Event: indexedmapapi.Event{
			Type:  indexedmapapi.Event_UPDATE,
			Entry: *m.newEntry(newEntry.IndexedMapEntry),
		},
	})

	return put.Reply(&indexedmapapi.PutResponse{
		Entry: m.newEntry(newEntry.IndexedMapEntry),
	})
}

func (m *indexedMapService) Get(get GetProposal) error {
	var entry *LinkedMapEntryValue
	var ok bool
	if get.Request().Position.Index > 0 {
		entry, ok = m.indexes[get.Request().Position.Index]
	} else {
		entry, ok = m.entries[get.Request().Position.Key]
	}

	if !ok {
		return get.Reply(&indexedmapapi.GetResponse{})
	}
	return get.Reply(&indexedmapapi.GetResponse{
		Entry: m.newEntry(entry.IndexedMapEntry),
	})
}

func (m *indexedMapService) FirstEntry(firstEntry FirstEntryProposal) error {
	if m.firstEntry == nil {
		return firstEntry.Reply(&indexedmapapi.FirstEntryResponse{})
	}
	return firstEntry.Reply(&indexedmapapi.FirstEntryResponse{
		Entry: m.newEntry(m.firstEntry.IndexedMapEntry),
	})
}

func (m *indexedMapService) LastEntry(lastEntry LastEntryProposal) error {
	if m.lastEntry == nil {
		return lastEntry.Reply(&indexedmapapi.LastEntryResponse{})
	}
	return lastEntry.Reply(&indexedmapapi.LastEntryResponse{
		Entry: m.newEntry(m.lastEntry.IndexedMapEntry),
	})
}

func (m *indexedMapService) PrevEntry(prevEntry PrevEntryProposal) error {
	entry, ok := m.indexes[prevEntry.Request().Index]
	if !ok {
		for _, e := range m.indexes {
			if entry == nil || (e.Index < prevEntry.Request().Index && e.Index > entry.Index) {
				entry = e
			}
		}
	} else {
		entry = entry.Prev
	}

	if entry == nil {
		return prevEntry.Reply(&indexedmapapi.PrevEntryResponse{})
	}
	return prevEntry.Reply(&indexedmapapi.PrevEntryResponse{
		Entry: m.newEntry(entry.IndexedMapEntry),
	})
}

func (m *indexedMapService) NextEntry(nextEntry NextEntryProposal) error {
	entry, ok := m.indexes[nextEntry.Request().Index]
	if !ok {
		for _, e := range m.indexes {
			if entry == nil || (e.Index > nextEntry.Request().Index && e.Index < entry.Index) {
				entry = e
			}
		}
	} else {
		entry = entry.Next
	}

	if entry == nil {
		return nextEntry.Reply(&indexedmapapi.NextEntryResponse{})
	}
	return nextEntry.Reply(&indexedmapapi.NextEntryResponse{
		Entry: m.newEntry(entry.IndexedMapEntry),
	})
}

func (m *indexedMapService) Remove(remove RemoveProposal) error {
	var entry *LinkedMapEntryValue
	if remove.Request().Entry.Index > 0 {
		entry = m.indexes[remove.Request().Entry.Index]
	} else {
		entry = m.entries[remove.Request().Entry.Key]
	}

	if entry == nil {
		return remove.Reply(&indexedmapapi.RemoveResponse{})
	}

	// Delete the entry from the map.
	delete(m.entries, entry.Key)
	delete(m.indexes, entry.Index)

	// Cancel any TTLs.
	m.cancelTTL(remove.Request().Entry.Key)

	// Update links for previous and next entries
	if entry.Prev != nil {
		entry.Prev.Next = entry.Next
	} else {
		m.firstEntry = entry.Next
	}
	if entry.Next != nil {
		entry.Next.Prev = entry.Prev
	} else {
		m.lastEntry = entry.Prev
	}

	m.notify(&indexedmapapi.EventsResponse{
		Event: indexedmapapi.Event{
			Type:  indexedmapapi.Event_REMOVE,
			Entry: *m.newEntry(entry.IndexedMapEntry),
		},
	})

	return remove.Reply(&indexedmapapi.RemoveResponse{
		Entry: m.newEntry(entry.IndexedMapEntry),
	})
}

func (m *indexedMapService) Clear(clear ClearProposal) error {
	m.entries = make(map[string]*LinkedMapEntryValue)
	m.indexes = make(map[uint64]*LinkedMapEntryValue)
	m.firstEntry = nil
	m.lastEntry = nil
	return clear.Reply(&indexedmapapi.ClearResponse{})
}

func (m *indexedMapService) Events(events EventsProposal) error {
	m.listeners[events.ID()] = &IndexedMapStateListener{
		ProposalID: events.ID(),
		Key:        events.Request().Pos.Key,
		Index:      events.Request().Pos.Index,
	}
	if events.Request().Replay {
		for _, entry := range m.entries {
			if (events.Request().Pos.Key == "" && events.Request().Pos.Index == 0) ||
				(events.Request().Pos.Key != "" && events.Request().Pos.Key == entry.Key) ||
				(events.Request().Pos.Index != 0 && events.Request().Pos.Index == entry.Index) {
				event := indexedmapapi.Event{
					Type:  indexedmapapi.Event_REPLAY,
					Entry: *m.newEntry(entry.IndexedMapEntry),
				}
				err := events.Notify(&indexedmapapi.EventsResponse{
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

func (m *indexedMapService) Entries(entries EntriesProposal) error {
	defer entries.Close()
	for _, entry := range m.entries {
		err := entries.Notify(&indexedmapapi.EntriesResponse{
			Entry: *m.newEntry(entry.IndexedMapEntry),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *indexedMapService) scheduleTTL(key string, entry *IndexedMapEntry) {
	m.cancelTTL(key)
	if entry.Expire != nil {
		m.timers[key] = m.Scheduler().RunAt(*entry.Expire, func() {
			delete(m.entries, key)
			m.notify(&indexedmapapi.EventsResponse{
				Event: indexedmapapi.Event{
					Type:  indexedmapapi.Event_REMOVE,
					Entry: *m.newEntry(entry),
				},
			})
		})
	}
}

func (m *indexedMapService) cancelTTL(key string) {
	timer, ok := m.timers[key]
	if ok {
		timer.Cancel()
	}
}

func (m *indexedMapService) newEntryState(entry *indexedmapapi.Entry) *IndexedMapEntry {
	state := &IndexedMapEntry{
		IndexedMapEntryPosition: IndexedMapEntryPosition{
			Index: entry.Position.Index,
			Key:   entry.Position.Key,
		},
		IndexedMapEntryValue: IndexedMapEntryValue{
			Value: entry.Value.Value,
		},
	}
	if entry.Value.TTL != nil {
		expire := m.Scheduler().Time().Add(*entry.Value.TTL)
		state.Expire = &expire
	}
	return state
}

func (m *indexedMapService) newEntry(state *IndexedMapEntry) *indexedmapapi.Entry {
	entry := &indexedmapapi.Entry{
		Position: indexedmapapi.Position{
			Index: state.Index,
			Key:   state.Key,
		},
		Value: indexedmapapi.Value{
			ObjectMeta: state.ObjectMeta,
			Value:      state.Value,
		},
	}
	if state.Expire != nil {
		ttl := m.Scheduler().Time().Sub(*state.Expire)
		entry.Value.TTL = &ttl
	}
	return entry
}

func checkPreconditions(entry *IndexedMapEntry, preconditions []indexedmapapi.Precondition) error {
	for _, precondition := range preconditions {
		switch p := precondition.Precondition.(type) {
		case *indexedmapapi.Precondition_Metadata:
			if p.Metadata.Type == metaapi.ObjectMeta_TOMBSTONE {
				if entry != nil {
					return errors.NewConflict("metadata precondition failed")
				}
			} else {
				if entry == nil {
					return errors.NewConflict("metadata precondition failed")
				}
				if !meta.Equal(entry.ObjectMeta, *p.Metadata) {
					return errors.NewConflict("metadata precondition failed")
				}
			}
		}
	}
	return nil
}

// LinkedMapEntryValue is a doubly linked MapEntryValue
type LinkedMapEntryValue struct {
	*IndexedMapEntry
	Prev *LinkedMapEntryValue
	Next *LinkedMapEntryValue
}
