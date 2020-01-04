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
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/atomix/atomix-go-node/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
)

func init() {
	node.RegisterService(indexMapType, newService)
}

// newService returns a new Service
func newService(context service.Context) service.Service {
	service := &Service{
		SessionizedService: service.NewSessionizedService(context),
		entries:            make(map[string]*LinkedMapEntryValue),
		indexes:            make(map[uint64]*LinkedMapEntryValue),
		timers:             make(map[string]service.Timer),
		listeners:          make(map[uint64]map[uint64]listener),
	}
	service.init()
	return service
}

// Service is a state machine for a map primitive
type Service struct {
	*service.SessionizedService
	lastIndex  uint64
	entries    map[string]*LinkedMapEntryValue
	indexes    map[uint64]*LinkedMapEntryValue
	firstEntry *LinkedMapEntryValue
	lastEntry  *LinkedMapEntryValue
	timers     map[string]service.Timer
	listeners  map[uint64]map[uint64]listener
}

// init initializes the map service
func (m *Service) init() {
	m.Executor.RegisterUnary(opPut, m.Put)
	m.Executor.RegisterUnary(opReplace, m.Replace)
	m.Executor.RegisterUnary(opRemove, m.Remove)
	m.Executor.RegisterUnary(opGet, m.Get)
	m.Executor.RegisterUnary(opFirstEntry, m.FirstEntry)
	m.Executor.RegisterUnary(opLastEntry, m.LastEntry)
	m.Executor.RegisterUnary(opPrevEntry, m.PrevEntry)
	m.Executor.RegisterUnary(opNextEntry, m.NextEntry)
	m.Executor.RegisterUnary(opExists, m.Exists)
	m.Executor.RegisterUnary(opSize, m.Size)
	m.Executor.RegisterUnary(opClear, m.Clear)
	m.Executor.RegisterStream(opEvents, m.Events)
	m.Executor.RegisterStream(opEntries, m.Entries)
}

// LinkedMapEntryValue is a doubly linked MapEntryValue
type LinkedMapEntryValue struct {
	*MapEntryValue
	Prev *LinkedMapEntryValue
	Next *LinkedMapEntryValue
}

// Backup backs up the map service
func (m *Service) Backup() ([]byte, error) {
	entries := make([]*MapEntryValue, len(m.entries))
	entry := m.firstEntry
	for entry != nil {
		entries = append(entries, entry.MapEntryValue)
		entry = entry.Next
	}

	listeners := make([]*Listener, 0)
	for sessionID, sessionListeners := range m.listeners {
		for streamID, sessionListener := range sessionListeners {
			listeners = append(listeners, &Listener{
				SessionId: sessionID,
				StreamId:  streamID,
				Key:       sessionListener.key,
				Index:     sessionListener.index,
			})
		}
	}

	snapshot := &MapSnapshot{
		Index:     m.lastIndex,
		Entries:   entries,
		Listeners: listeners,
	}
	return proto.Marshal(snapshot)
}

// Restore restores the map service
func (m *Service) Restore(bytes []byte) error {
	snapshot := &MapSnapshot{}
	if err := proto.Unmarshal(bytes, snapshot); err != nil {
		return err
	}

	m.lastIndex = snapshot.Index
	m.firstEntry = nil
	m.lastEntry = nil
	m.entries = make(map[string]*LinkedMapEntryValue)
	m.indexes = make(map[uint64]*LinkedMapEntryValue)

	var prevEntry *LinkedMapEntryValue
	for _, entry := range snapshot.Entries {
		linkedEntry := &LinkedMapEntryValue{
			MapEntryValue: entry,
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
	}

	m.listeners = make(map[uint64]map[uint64]listener)
	for _, snapshotListener := range snapshot.Listeners {
		sessionListeners, ok := m.listeners[snapshotListener.SessionId]
		if !ok {
			sessionListeners = make(map[uint64]listener)
			m.listeners[snapshotListener.SessionId] = sessionListeners
		}
		sessionListeners[snapshotListener.StreamId] = listener{
			key:    snapshotListener.Key,
			stream: m.Sessions()[snapshotListener.SessionId].Stream(snapshotListener.StreamId),
		}
	}

	// TODO: Restore TTLs!
	return nil
}

// Put puts a key/value pair in the map
func (m *Service) Put(value []byte) ([]byte, error) {
	request := &PutRequest{}
	if err := proto.Unmarshal(value, request); err != nil {
		return nil, err
	}

	var oldEntry *LinkedMapEntryValue
	if request.Index > 0 {
		oldEntry = m.indexes[request.Index]
		if oldEntry == nil {
			return proto.Marshal(&PutResponse{
				Status: UpdateStatus_PRECONDITION_FAILED,
			})
		}
	} else {
		oldEntry = m.entries[request.Key]
	}

	if oldEntry == nil {
		// If the version is positive then reject the request.
		if !request.IfEmpty && request.Version > 0 {
			return proto.Marshal(&PutResponse{
				Status: UpdateStatus_PRECONDITION_FAILED,
			})
		}

		// Increment the index for a new entry
		m.lastIndex++
		nextIndex := m.lastIndex

		// Create a new entry value and set it in the map.
		newEntry := &LinkedMapEntryValue{
			MapEntryValue: &MapEntryValue{
				Index:   nextIndex,
				Key:     request.Key,
				Value:   request.Value,
				Version: m.Context.Index(),
				TTL:     request.TTL,
				Created: m.Context.Timestamp(),
				Updated: m.Context.Timestamp(),
			},
		}
		m.entries[newEntry.Key] = newEntry
		m.indexes[newEntry.Index] = newEntry

		// Set the first entry if not set
		if m.firstEntry == nil {
			m.firstEntry = newEntry
		}

		// If the last entry is set, link it to the new entry
		if m.lastEntry != nil {
			m.lastEntry.Next = newEntry
			newEntry.Prev = m.lastEntry
		}

		// Update the last entry
		m.lastEntry = newEntry

		// Schedule the timeout for the value if necessary.
		m.scheduleTTL(request.Key, newEntry)

		// Publish an event to listener streams.
		m.sendEvent(&ListenResponse{
			Type:    ListenResponse_INSERTED,
			Key:     request.Key,
			Index:   newEntry.Index,
			Value:   newEntry.Value,
			Version: newEntry.Version,
			Created: newEntry.Created,
			Updated: newEntry.Updated,
		})

		return proto.Marshal(&PutResponse{
			Status: UpdateStatus_OK,
			Index:  newEntry.Index,
			Key:    newEntry.Key,
		})
	}

	// If the version is -1 then reject the request.
	// If the version is positive then compare the version to the current version.
	if request.IfEmpty || (!request.IfEmpty && request.Version > 0 && request.Version != oldEntry.Version) {
		return proto.Marshal(&PutResponse{
			Status:          UpdateStatus_PRECONDITION_FAILED,
			Index:           oldEntry.Index,
			Key:             oldEntry.Key,
			PreviousValue:   oldEntry.Value,
			PreviousVersion: oldEntry.Version,
		})
	}

	// If the value is equal to the current value, return a no-op.
	if bytes.Equal(oldEntry.Value, request.Value) {
		return proto.Marshal(&PutResponse{
			Status:          UpdateStatus_NOOP,
			Index:           oldEntry.Index,
			Key:             oldEntry.Key,
			PreviousValue:   oldEntry.Value,
			PreviousVersion: oldEntry.Version,
		})
	}

	// Create a new entry value and set it in the map.
	newEntry := &LinkedMapEntryValue{
		MapEntryValue: &MapEntryValue{
			Index:   oldEntry.Index,
			Key:     oldEntry.Key,
			Value:   request.Value,
			Version: m.Context.Index(),
			TTL:     request.TTL,
			Created: oldEntry.Created,
			Updated: m.Context.Timestamp(),
		},
		Prev: oldEntry.Prev,
		Next: oldEntry.Next,
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
	m.scheduleTTL(request.Key, newEntry)

	// Publish an event to listener streams.
	m.sendEvent(&ListenResponse{
		Type:    ListenResponse_UPDATED,
		Key:     request.Key,
		Index:   newEntry.Index,
		Value:   newEntry.Value,
		Version: newEntry.Version,
		Created: newEntry.Created,
		Updated: newEntry.Updated,
	})

	return proto.Marshal(&PutResponse{
		Status:          UpdateStatus_OK,
		Index:           newEntry.Index,
		Key:             newEntry.Key,
		PreviousValue:   oldEntry.Value,
		PreviousVersion: oldEntry.Version,
		Created:         newEntry.Created,
		Updated:         newEntry.Updated,
	})
}

// Replace replaces a key/value pair in the map
func (m *Service) Replace(value []byte) ([]byte, error) {
	request := &ReplaceRequest{}
	if err := proto.Unmarshal(value, request); err != nil {
		return nil, err
	}

	var oldEntry *LinkedMapEntryValue
	if request.Index > 0 {
		oldEntry = m.indexes[request.Index]
	} else {
		oldEntry = m.entries[request.Key]
	}

	if oldEntry == nil {
		return proto.Marshal(&ReplaceResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		})
	}

	// If the version was specified and does not match the entry version, fail the replace.
	if request.PreviousVersion != 0 && request.PreviousVersion != oldEntry.Version {
		return proto.Marshal(&ReplaceResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		})
	}

	// If the value was specified and does not match the entry value, fail the replace.
	if len(request.PreviousValue) != 0 && bytes.Equal(request.PreviousValue, oldEntry.Value) {
		return proto.Marshal(&ReplaceResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		})
	}

	// If we've made it this far, update the entry.
	// Create a new entry value and set it in the map.
	newEntry := &LinkedMapEntryValue{
		MapEntryValue: &MapEntryValue{
			Index:   oldEntry.Index,
			Key:     oldEntry.Key,
			Value:   request.NewValue,
			Version: m.Context.Index(),
			TTL:     request.TTL,
			Created: oldEntry.Created,
			Updated: m.Context.Timestamp(),
		},
		Prev: oldEntry.Prev,
		Next: oldEntry.Next,
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
	m.scheduleTTL(request.Key, newEntry)

	// Publish an event to listener streams.
	m.sendEvent(&ListenResponse{
		Type:    ListenResponse_UPDATED,
		Key:     request.Key,
		Index:   newEntry.Index,
		Value:   newEntry.Value,
		Version: newEntry.Version,
		Created: newEntry.Created,
		Updated: newEntry.Updated,
	})

	return proto.Marshal(&ReplaceResponse{
		Status:          UpdateStatus_OK,
		Index:           newEntry.Index,
		Key:             newEntry.Key,
		PreviousValue:   oldEntry.Value,
		PreviousVersion: oldEntry.Version,
		NewVersion:      newEntry.Version,
		Created:         newEntry.Created,
		Updated:         newEntry.Updated,
	})
}

// Remove removes a key/value pair from the map
func (m *Service) Remove(bytes []byte) ([]byte, error) {
	request := &RemoveRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	var entry *LinkedMapEntryValue
	if request.Index > 0 {
		entry = m.indexes[request.Index]
	} else {
		entry = m.entries[request.Key]
	}

	if entry == nil {
		return proto.Marshal(&RemoveResponse{
			Status: UpdateStatus_NOOP,
		})
	}

	// If the request version is set, verify that the request version matches the entry version.
	if request.Version > 0 && request.Version != entry.Version {
		return proto.Marshal(&RemoveResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		})
	}

	// Delete the entry from the map.
	delete(m.entries, entry.Key)
	delete(m.indexes, entry.Index)

	// Cancel any TTLs.
	m.cancelTTL(request.Key)

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

	// Publish an event to listener streams.
	m.sendEvent(&ListenResponse{
		Type:    ListenResponse_REMOVED,
		Key:     entry.Key,
		Index:   entry.Index,
		Value:   entry.Value,
		Version: entry.Version,
		Created: entry.Created,
		Updated: entry.Updated,
	})

	return proto.Marshal(&ReplaceResponse{
		Status:          UpdateStatus_OK,
		Index:           entry.Index,
		Key:             entry.Key,
		PreviousValue:   entry.Value,
		PreviousVersion: entry.Version,
		Created:         entry.Created,
		Updated:         entry.Updated,
	})
}

// Get gets a value from the map
func (m *Service) Get(bytes []byte) ([]byte, error) {
	request := &GetRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	var entry *LinkedMapEntryValue
	var ok bool
	if request.Index > 0 {
		entry, ok = m.indexes[request.Index]
	} else {
		entry, ok = m.entries[request.Key]
	}

	if !ok {
		return proto.Marshal(&GetResponse{})
	} else {
		return proto.Marshal(&GetResponse{
			Index:   entry.Index,
			Key:     entry.Key,
			Value:   entry.Value,
			Version: entry.Version,
			Created: entry.Created,
			Updated: entry.Updated,
		})
	}
}

// FirstEntry gets the first entry from the map
func (m *Service) FirstEntry(bytes []byte) ([]byte, error) {
	request := &FirstEntryRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	if m.firstEntry == nil {
		return proto.Marshal(&FirstEntryResponse{})
	} else {
		return proto.Marshal(&FirstEntryResponse{
			Index:   m.firstEntry.Index,
			Key:     m.firstEntry.Key,
			Value:   m.firstEntry.Value,
			Version: m.firstEntry.Version,
			Created: m.firstEntry.Created,
			Updated: m.firstEntry.Updated,
		})
	}
}

// LastEntry gets the last entry from the map
func (m *Service) LastEntry(bytes []byte) ([]byte, error) {
	request := &LastEntryRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	if m.lastEntry == nil {
		return proto.Marshal(&LastEntryResponse{})
	} else {
		return proto.Marshal(&LastEntryResponse{
			Index:   m.lastEntry.Index,
			Key:     m.lastEntry.Key,
			Value:   m.lastEntry.Value,
			Version: m.lastEntry.Version,
			Created: m.lastEntry.Created,
			Updated: m.lastEntry.Updated,
		})
	}
}

// PrevEntry gets the previous entry from the map
func (m *Service) PrevEntry(bytes []byte) ([]byte, error) {
	request := &PrevEntryRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
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
		return proto.Marshal(&PrevEntryResponse{})
	} else {
		return proto.Marshal(&PrevEntryResponse{
			Index:   entry.Index,
			Key:     entry.Key,
			Value:   entry.Value,
			Version: entry.Version,
			Created: entry.Created,
			Updated: entry.Updated,
		})
	}
}

// NextEntry gets the next entry from the map
func (m *Service) NextEntry(bytes []byte) ([]byte, error) {
	request := &NextEntryRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
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
		return proto.Marshal(&NextEntryResponse{})
	} else {
		return proto.Marshal(&NextEntryResponse{
			Index:   entry.Index,
			Key:     entry.Key,
			Value:   entry.Value,
			Version: entry.Version,
			Created: entry.Created,
			Updated: entry.Updated,
		})
	}
}

// Exists checks if the map contains a key
func (m *Service) Exists(bytes []byte) ([]byte, error) {
	request := &ContainsKeyRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	_, ok := m.entries[request.Key]
	return proto.Marshal(&ContainsKeyResponse{
		ContainsKey: ok,
	})
}

// Size returns the size of the map
func (m *Service) Size(bytes []byte) ([]byte, error) {
	return proto.Marshal(&SizeResponse{
		Size_: int32(len(m.entries)),
	})
}

// Clear removes all entries from the map
func (m *Service) Clear(value []byte) ([]byte, error) {
	m.entries = make(map[string]*LinkedMapEntryValue)
	m.indexes = make(map[uint64]*LinkedMapEntryValue)
	m.firstEntry = nil
	m.lastEntry = nil
	return proto.Marshal(&ClearResponse{})
}

// Events sends change events to the client
func (m *Service) Events(bytes []byte, stream stream.Stream) {
	request := &ListenRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		stream.Error(err)
		stream.Close()
		return
	}

	// Send an OPEN response to notify the client the stream is open
	stream.Result(proto.Marshal(&ListenResponse{
		Type: ListenResponse_OPEN,
	}))

	// Create and populate the listener
	l := listener{
		key:    request.Key,
		index:  request.Index,
		stream: stream,
	}
	listeners, ok := m.listeners[m.Session().ID]
	if !ok {
		listeners = make(map[uint64]listener)
		m.listeners[m.Session().ID] = listeners
	}
	listeners[m.Session().StreamID()] = l

	if request.Replay {
		entry := m.firstEntry
		for entry != nil {
			stream.Result(proto.Marshal(&ListenResponse{
				Type:    ListenResponse_NONE,
				Key:     entry.Key,
				Index:   entry.Index,
				Value:   entry.Value,
				Version: entry.Version,
				Created: entry.Created,
				Updated: entry.Updated,
			}))
			entry = entry.Next
		}
	}
}

// Entries returns a stream of entries to the client
func (m *Service) Entries(value []byte, stream stream.Stream) {
	defer stream.Close()
	entry := m.firstEntry
	for entry != nil {
		stream.Result(proto.Marshal(&EntriesResponse{
			Key:     entry.Key,
			Index:   entry.Index,
			Value:   entry.Value,
			Version: entry.Version,
			Created: entry.Created,
			Updated: entry.Updated,
		}))
		entry = entry.Next
	}
}

func (m *Service) scheduleTTL(key string, entry *LinkedMapEntryValue) {
	m.cancelTTL(key)
	if entry.TTL != nil {
		m.timers[key] = m.Scheduler.ScheduleOnce(entry.Created.Add(*entry.TTL).Sub(m.Context.Timestamp()), func() {
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

			m.sendEvent(&ListenResponse{
				Type:    ListenResponse_REMOVED,
				Key:     key,
				Index:   entry.Index,
				Value:   entry.Value,
				Version: uint64(entry.Version),
				Created: entry.Created,
				Updated: entry.Updated,
			})
		})
	}
}

func (m *Service) cancelTTL(key string) {
	timer, ok := m.timers[key]
	if ok {
		timer.Cancel()
	}
}

func (m *Service) sendEvent(event *ListenResponse) {
	bytes, _ := proto.Marshal(event)
	for sessionID, listeners := range m.listeners {
		session := m.Sessions()[sessionID]
		if session != nil {
			for _, listener := range listeners {
				if listener.key != "" {
					if event.Key == listener.key {
						listener.stream.Value(bytes)
					}
				} else if listener.index > 0 {
					if event.Index == listener.index {
						listener.stream.Value(bytes)
					}
				} else {
					listener.stream.Value(bytes)
				}
			}
		}
	}
}

type listener struct {
	key    string
	index  uint64
	stream stream.Stream
}