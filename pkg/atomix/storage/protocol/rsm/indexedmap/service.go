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

func (m *indexedMapService) notify(event *indexedmapapi.EventsResponse) error {
	for proposalID, listener := range m.listeners {
		if (listener.Key == "" && listener.Index == 0) ||
			(listener.Key != "" && listener.Key == event.Event.Entry.Key) ||
			(listener.Index != 0 && listener.Index == event.Event.Entry.Index) {
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

func (m *indexedMapService) Size(size SizeQuery) error {
	return size.Reply(&indexedmapapi.SizeResponse{
		Size_: uint32(len(m.entries)),
	})
}

func (m *indexedMapService) Put(put PutProposal) error {
	request, err := put.Request()
	if err != nil {
		return err
	}

	var oldEntry *LinkedMapEntryValue
	if request.Entry.Index > 0 {
		oldEntry = m.indexes[request.Entry.Index]
		if oldEntry != nil && oldEntry.Key != request.Entry.Key {
			return errors.NewAlreadyExists("entry already exists at index %d with key %s", request.Entry.Index, request.Entry.Key)
		}
	} else {
		oldEntry = m.entries[request.Entry.Key]
	}

	var entry *IndexedMapEntry
	if oldEntry != nil {
		entry = oldEntry.IndexedMapEntry
	}
	if err := checkPreconditions(entry, request.Preconditions); err != nil {
		return err
	}

	if oldEntry == nil {
		// Increment the index for a new entry
		var index uint64
		if request.Entry.Index > 0 {
			if request.Entry.Index > m.lastIndex {
				m.lastIndex = request.Entry.Index
			}
			index = request.Entry.Index
		} else {
			m.lastIndex++
			index = m.lastIndex
		}

		// Create a new entry value and set it in the map.
		newEntry := &LinkedMapEntryValue{
			IndexedMapEntry: &IndexedMapEntry{
				IndexedMapEntryPosition: IndexedMapEntryPosition{
					Index: index,
					Key:   request.Entry.Key,
				},
				IndexedMapEntryValue: IndexedMapEntryValue{
					ObjectMeta: metaapi.ObjectMeta{
						Revision: &metaapi.Revision{
							Num: metaapi.RevisionNum(put.ID()),
						},
					},
					Value: request.Entry.Value.Value,
				},
			},
		}
		if request.Entry.TTL != nil {
			expire := m.Scheduler().Time().Add(*request.Entry.TTL)
			newEntry.Expire = &expire
		}
		m.entries[newEntry.Key] = newEntry
		m.indexes[newEntry.Index] = newEntry

		// Set the first entry if not set
		if m.firstEntry == nil {
			m.firstEntry = newEntry
		}

		// If the last entry is set, link it to the new entry
		if request.Entry.Index > 0 {
			if m.lastIndex == request.Entry.Index {
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
		m.scheduleTTL(request.Entry.Key, newEntry.IndexedMapEntry.Expire)

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
	if bytes.Equal(oldEntry.Value, request.Entry.Value.Value) {
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
				ObjectMeta: metaapi.ObjectMeta{
					Revision: &metaapi.Revision{
						Num: metaapi.RevisionNum(put.ID()),
					},
				},
				Value: request.Entry.Value.Value,
			},
		},
		Prev: oldEntry.Prev,
		Next: oldEntry.Next,
	}
	if request.Entry.TTL != nil {
		expire := m.Scheduler().Time().Add(*request.Entry.TTL)
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

	return put.Reply(&indexedmapapi.PutResponse{
		Entry: m.newEntry(newEntry.IndexedMapEntry),
	})
}

func (m *indexedMapService) Get(get GetQuery) error {
	request, err := get.Request()
	if err != nil {
		return err
	}

	var entry *LinkedMapEntryValue
	var ok bool
	if request.Position.Index > 0 {
		entry, ok = m.indexes[request.Position.Index]
		if entry == nil {
			return errors.NewNotFound("no entry found at index %d", request.Position.Index)
		}
	} else {
		entry, ok = m.entries[request.Position.Key]
		if entry == nil {
			return errors.NewNotFound("no entry found at key %s", request.Position.Key)
		}
	}

	if !ok {
		return errors.NewNotFound("entry not found")
	}
	return get.Reply(&indexedmapapi.GetResponse{
		Entry: m.newEntry(entry.IndexedMapEntry),
	})
}

func (m *indexedMapService) FirstEntry(firstEntry FirstEntryQuery) error {
	if m.firstEntry == nil {
		return errors.NewNotFound("map is empty")
	}
	return firstEntry.Reply(&indexedmapapi.FirstEntryResponse{
		Entry: m.newEntry(m.firstEntry.IndexedMapEntry),
	})
}

func (m *indexedMapService) LastEntry(lastEntry LastEntryQuery) error {
	if m.lastEntry == nil {
		return errors.NewNotFound("map is empty")
	}
	return lastEntry.Reply(&indexedmapapi.LastEntryResponse{
		Entry: m.newEntry(m.lastEntry.IndexedMapEntry),
	})
}

func (m *indexedMapService) PrevEntry(prevEntry PrevEntryQuery) error {
	request, err := prevEntry.Request()
	if err != nil {
		return err
	}

	entry, ok := m.indexes[request.Index]
	if !ok {
		for _, e := range m.indexes {
			if entry == nil || (e.Index < request.Index && e.Index > entry.Index) {
				entry = e
			}
		}
	} else {
		entry = entry.Prev
	}

	if entry == nil {
		return errors.NewNotFound("no entry found prior to index %d", request.Index)
	}
	return prevEntry.Reply(&indexedmapapi.PrevEntryResponse{
		Entry: m.newEntry(entry.IndexedMapEntry),
	})
}

func (m *indexedMapService) NextEntry(nextEntry NextEntryQuery) error {
	request, err := nextEntry.Request()
	if err != nil {
		return err
	}

	entry, ok := m.indexes[request.Index]
	if !ok {
		for _, e := range m.indexes {
			if entry == nil || (e.Index > request.Index && e.Index < entry.Index) {
				entry = e
			}
		}
	} else {
		entry = entry.Next
	}

	if entry == nil {
		return errors.NewNotFound("no entry found after index %d", request.Index)
	}
	return nextEntry.Reply(&indexedmapapi.NextEntryResponse{
		Entry: m.newEntry(entry.IndexedMapEntry),
	})
}

func (m *indexedMapService) Remove(remove RemoveProposal) error {
	request, err := remove.Request()
	if err != nil {
		return err
	}

	var entry *LinkedMapEntryValue
	if request.Entry.Index > 0 {
		entry = m.indexes[request.Entry.Index]
		if entry == nil {
			return errors.NewNotFound("no entry found at index %d", request.Entry.Index)
		}
	} else {
		entry = m.entries[request.Entry.Key]
		if entry == nil {
			return errors.NewNotFound("no entry found at key %s", request.Entry.Key)
		}
	}

	if err := checkPreconditions(entry.IndexedMapEntry, request.Preconditions); err != nil {
		return err
	}

	// Delete the entry from the map.
	delete(m.entries, entry.Key)
	delete(m.indexes, entry.Index)

	// Cancel any TTLs.
	m.cancelTTL(request.Entry.Key)

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
	request, err := events.Request()
	if err != nil {
		return err
	}

	listener := &IndexedMapStateListener{
		ProposalID: events.ID(),
		Key:        request.Pos.Key,
		Index:      request.Pos.Index,
	}
	m.listeners[events.ID()] = listener
	if request.Replay {
		entry := m.firstEntry
		for entry != nil {
			if (listener.Key == "" && listener.Index == 0) ||
				(listener.Key != "" && listener.Key == entry.Key) ||
				(listener.Index != 0 && listener.Index == entry.Index) {
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
			entry = entry.Next
		}
	}
	return nil
}

func (m *indexedMapService) Entries(entries EntriesQuery) error {
	defer entries.Close()
	entry := m.firstEntry
	for entry != nil {
		err := entries.Notify(&indexedmapapi.EntriesResponse{
			Entry: *m.newEntry(entry.IndexedMapEntry),
		})
		if err != nil {
			return err
		}
		entry = entry.Next
	}
	return nil
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
