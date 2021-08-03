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
	"time"
)

func init() {
	registerServiceFunc(newService)
}

func newService(context ServiceContext) Service {
	return &indexedMapService{
		ServiceContext: context,
		entries:        make(map[string]*LinkedMapEntryValue),
		indexes:        make(map[uint64]*LinkedMapEntryValue),
		listeners:      make(map[ProposalID]*IndexedMapStateListener),
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
	return writer.WriteState(&IndexedMapState{
		Listeners: listeners,
		Entries:   entries,
	})
}

func (m *indexedMapService) Restore(reader SnapshotReader) error {
	state, err := reader.ReadState()
	if err != nil {
		return err
	}
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
		m.scheduleTTL(entry.Key, entry.Expire)
	}
	return nil
}

func (m *indexedMapService) notify(event *indexedmapapi.EventsResponse) {
	for proposalID, listener := range m.listeners {
		if (listener.Key == "" && listener.Index == 0) ||
			(listener.Key != "" && listener.Key == event.Event.Entry.Key) ||
			(listener.Index != 0 && listener.Index == event.Event.Entry.Index) {
			proposal, ok := m.Proposals().Events().Get(proposalID)
			if ok {
				proposal.Notify(event)
			} else {
				delete(m.listeners, proposalID)
			}
		}
	}
}

func (m *indexedMapService) Size(size SizeQuery) (*indexedmapapi.SizeResponse, error) {
	return &indexedmapapi.SizeResponse{
		Size_: uint32(len(m.entries)),
	}, nil
}

func (m *indexedMapService) Put(put PutProposal) (*indexedmapapi.PutResponse, error) {
	var oldEntry *LinkedMapEntryValue
	if put.Request().Entry.Index > 0 {
		oldEntry = m.indexes[put.Request().Entry.Index]
		if oldEntry != nil && oldEntry.Key != put.Request().Entry.Key {
			return nil, errors.NewAlreadyExists("entry already exists at index %d with key %s", put.Request().Entry.Index, put.Request().Entry.Key)
		}
	} else {
		oldEntry = m.entries[put.Request().Entry.Key]
	}

	var entry *IndexedMapEntry
	if oldEntry != nil {
		entry = oldEntry.IndexedMapEntry
	}
	if err := checkPreconditions(entry, put.Request().Preconditions); err != nil {
		return nil, err
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
					ObjectMeta: metaapi.ObjectMeta{
						Revision: &metaapi.Revision{
							Num: metaapi.RevisionNum(put.ID()),
						},
					},
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
		m.scheduleTTL(put.Request().Entry.Key, newEntry.IndexedMapEntry.Expire)

		m.notify(&indexedmapapi.EventsResponse{
			Event: indexedmapapi.Event{
				Type:  indexedmapapi.Event_INSERT,
				Entry: *m.newEntry(newEntry.IndexedMapEntry),
			},
		})

		return &indexedmapapi.PutResponse{
			Entry: m.newEntry(newEntry.IndexedMapEntry),
		}, nil
	}

	// If the value is equal to the current value, return a no-op.
	if bytes.Equal(oldEntry.Value, put.Request().Entry.Value.Value) {
		return &indexedmapapi.PutResponse{
			Entry: m.newEntry(oldEntry.IndexedMapEntry),
		}, nil
	}

	// Create a new entry value and set it in the map.
	newEntry := &LinkedMapEntryValue{
		IndexedMapEntry: &IndexedMapEntry{
			IndexedMapEntryPosition: IndexedMapEntryPosition{
				Index: oldEntry.Index,
				Key:   oldEntry.Key,
			},
			IndexedMapEntryValue: IndexedMapEntryValue{
				ObjectMeta: metaapi.ObjectMeta{
					Revision: &metaapi.Revision{
						Num: metaapi.RevisionNum(put.ID()),
					},
				},
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
	m.scheduleTTL(newEntry.Key, newEntry.IndexedMapEntry.Expire)

	m.notify(&indexedmapapi.EventsResponse{
		Event: indexedmapapi.Event{
			Type:  indexedmapapi.Event_UPDATE,
			Entry: *m.newEntry(newEntry.IndexedMapEntry),
		},
	})

	return &indexedmapapi.PutResponse{
		Entry: m.newEntry(newEntry.IndexedMapEntry),
	}, nil
}

func (m *indexedMapService) Get(get GetQuery) (*indexedmapapi.GetResponse, error) {
	var entry *LinkedMapEntryValue
	var ok bool
	if get.Request().Position.Index > 0 {
		entry, ok = m.indexes[get.Request().Position.Index]
		if entry == nil {
			return nil, errors.NewNotFound("no entry found at index %d", get.Request().Position.Index)
		}
	} else {
		entry, ok = m.entries[get.Request().Position.Key]
		if entry == nil {
			return nil, errors.NewNotFound("no entry found at key %s", get.Request().Position.Key)
		}
	}

	if !ok {
		return nil, errors.NewNotFound("entry not found")
	}
	return &indexedmapapi.GetResponse{
		Entry: m.newEntry(entry.IndexedMapEntry),
	}, nil
}

func (m *indexedMapService) FirstEntry(FirstEntryQuery) (*indexedmapapi.FirstEntryResponse, error) {
	if m.firstEntry == nil {
		return nil, errors.NewNotFound("map is empty")
	}
	return &indexedmapapi.FirstEntryResponse{
		Entry: m.newEntry(m.firstEntry.IndexedMapEntry),
	}, nil
}

func (m *indexedMapService) LastEntry(LastEntryQuery) (*indexedmapapi.LastEntryResponse, error) {
	if m.lastEntry == nil {
		return nil, errors.NewNotFound("map is empty")
	}
	return &indexedmapapi.LastEntryResponse{
		Entry: m.newEntry(m.lastEntry.IndexedMapEntry),
	}, nil
}

func (m *indexedMapService) PrevEntry(prevEntry PrevEntryQuery) (*indexedmapapi.PrevEntryResponse, error) {
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
		return nil, errors.NewNotFound("no entry found prior to index %d", prevEntry.Request().Index)
	}
	return &indexedmapapi.PrevEntryResponse{
		Entry: m.newEntry(entry.IndexedMapEntry),
	}, nil
}

func (m *indexedMapService) NextEntry(nextEntry NextEntryQuery) (*indexedmapapi.NextEntryResponse, error) {
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
		return nil, errors.NewNotFound("no entry found after index %d", nextEntry.Request().Index)
	}
	return &indexedmapapi.NextEntryResponse{
		Entry: m.newEntry(entry.IndexedMapEntry),
	}, nil
}

func (m *indexedMapService) Remove(remove RemoveProposal) (*indexedmapapi.RemoveResponse, error) {
	var entry *LinkedMapEntryValue
	if remove.Request().Entry.Index > 0 {
		entry = m.indexes[remove.Request().Entry.Index]
		if entry == nil {
			return nil, errors.NewNotFound("no entry found at index %d", remove.Request().Entry.Index)
		}
	} else {
		entry = m.entries[remove.Request().Entry.Key]
		if entry == nil {
			return nil, errors.NewNotFound("no entry found at key %s", remove.Request().Entry.Key)
		}
	}

	if err := checkPreconditions(entry.IndexedMapEntry, remove.Request().Preconditions); err != nil {
		return nil, err
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

	return &indexedmapapi.RemoveResponse{
		Entry: m.newEntry(entry.IndexedMapEntry),
	}, nil
}

func (m *indexedMapService) Clear(ClearProposal) (*indexedmapapi.ClearResponse, error) {
	m.entries = make(map[string]*LinkedMapEntryValue)
	m.indexes = make(map[uint64]*LinkedMapEntryValue)
	m.firstEntry = nil
	m.lastEntry = nil
	return &indexedmapapi.ClearResponse{}, nil
}

func (m *indexedMapService) Events(events EventsProposal) {
	listener := &IndexedMapStateListener{
		ProposalID: events.ID(),
		Key:        events.Request().Pos.Key,
		Index:      events.Request().Pos.Index,
	}
	m.listeners[events.ID()] = listener

	events.Notify(&indexedmapapi.EventsResponse{})

	if events.Request().Replay {
		entry := m.firstEntry
		for entry != nil {
			if (listener.Key == "" && listener.Index == 0) ||
				(listener.Key != "" && listener.Key == entry.Key) ||
				(listener.Index != 0 && listener.Index == entry.Index) {
				event := indexedmapapi.Event{
					Type:  indexedmapapi.Event_REPLAY,
					Entry: *m.newEntry(entry.IndexedMapEntry),
				}
				events.Notify(&indexedmapapi.EventsResponse{
					Event: event,
				})
			}
			entry = entry.Next
		}
	}
}

func (m *indexedMapService) Entries(entries EntriesQuery) {
	defer entries.Close()
	entry := m.firstEntry
	for entry != nil {
		entries.Notify(&indexedmapapi.EntriesResponse{
			Entry: *m.newEntry(entry.IndexedMapEntry),
		})
		entry = entry.Next
	}
}

func (m *indexedMapService) scheduleTTL(key string, expire *time.Time) {
	m.cancelTTL(key)
	if expire != nil {
		m.timers[key] = m.Scheduler().RunAt(*expire, func() {
			entry, ok := m.entries[key]
			if ok {
				delete(m.entries, key)
				delete(m.indexes, entry.Index)

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
			}
		})
	}
}

func (m *indexedMapService) cancelTTL(key string) {
	timer, ok := m.timers[key]
	if ok {
		timer.Cancel()
	}
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
