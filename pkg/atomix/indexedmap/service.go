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
	"github.com/golang/protobuf/proto"
)

func init() {
	node.RegisterService("indexedmap", newService)
}

// newService returns a new Service
func newService(context service.Context) service.Service {
	service := &Service{
		SessionizedService: service.NewSessionizedService(context),
		entries:            make(map[string]*LinkedMapEntryValue),
		indexes:            make(map[uint64]*LinkedMapEntryValue),
		timers:             make(map[string]service.Timer),
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
}

// init initializes the map service
func (m *Service) init() {
	m.Executor.Register("put", m.Put)
	m.Executor.Register("replace", m.Replace)
	m.Executor.Register("remove", m.Remove)
	m.Executor.Register("get", m.Get)
	m.Executor.Register("exists", m.ContainsKey)
	m.Executor.Register("size", m.Size)
	m.Executor.Register("clear", m.Clear)
	m.Executor.Register("events", m.Events)
	m.Executor.Register("entries", m.Entries)
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
	snapshot := &MapSnapshot{
		Index:   m.lastIndex,
		Entries: entries,
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

	// TODO: Restore TTLs!
	return nil
}

// Put puts a key/value pair in the map
func (m *Service) Put(value []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &PutRequest{}
	if err := proto.Unmarshal(value, request); err != nil {
		ch <- m.NewFailure(err)
		return
	}

	var oldEntry *LinkedMapEntryValue
	if request.Index > 0 {
		oldEntry = m.indexes[request.Index]
		if oldEntry == nil {
			ch <- m.NewResult(proto.Marshal(&PutResponse{
				Status: UpdateStatus_PRECONDITION_FAILED,
			}))
			return
		}
	} else {
		oldEntry = m.entries[request.Key]
	}

	if oldEntry == nil {
		// If the version is positive then reject the request.
		if !request.IfEmpty && request.Version > 0 {
			ch <- m.NewResult(proto.Marshal(&PutResponse{
				Status: UpdateStatus_PRECONDITION_FAILED,
			}))
			return
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
		m.entries[request.Key] = newEntry
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

		ch <- m.NewResult(proto.Marshal(&PutResponse{
			Status: UpdateStatus_OK,
			Index:  newEntry.Index,
			Key:    newEntry.Key,
		}))
		return
	}

	// If the version is -1 then reject the request.
	// If the version is positive then compare the version to the current version.
	if request.IfEmpty || (!request.IfEmpty && request.Version > 0 && request.Version != oldEntry.Version) {
		ch <- m.NewResult(proto.Marshal(&PutResponse{
			Status:          UpdateStatus_PRECONDITION_FAILED,
			Index:           oldEntry.Index,
			Key:             oldEntry.Key,
			PreviousValue:   oldEntry.Value,
			PreviousVersion: oldEntry.Version,
		}))
		return
	}

	// If the value is equal to the current value, return a no-op.
	if bytes.Equal(oldEntry.Value, request.Value) {
		ch <- m.NewResult(proto.Marshal(&PutResponse{
			Status:          UpdateStatus_NOOP,
			Index:           oldEntry.Index,
			Key:             oldEntry.Key,
			PreviousValue:   oldEntry.Value,
			PreviousVersion: oldEntry.Version,
		}))
		return
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
	m.entries[request.Key] = newEntry
	m.indexes[newEntry.Index] = newEntry

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

	ch <- m.NewResult(proto.Marshal(&PutResponse{
		Status:          UpdateStatus_OK,
		Index:           newEntry.Index,
		Key:             newEntry.Key,
		PreviousValue:   oldEntry.Value,
		PreviousVersion: oldEntry.Version,
		Created:         newEntry.Created,
		Updated:         newEntry.Updated,
	}))
}

// Replace replaces a key/value pair in the map
func (m *Service) Replace(value []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &ReplaceRequest{}
	if err := proto.Unmarshal(value, request); err != nil {
		ch <- m.NewFailure(err)
		return
	}

	var oldEntry *LinkedMapEntryValue
	if request.Index > 0 {
		oldEntry = m.indexes[request.Index]
	} else {
		oldEntry = m.entries[request.Key]
	}

	if oldEntry == nil {
		ch <- m.NewResult(proto.Marshal(&ReplaceResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		}))
		return
	}

	// If the version was specified and does not match the entry version, fail the replace.
	if request.PreviousVersion != 0 && request.PreviousVersion != oldEntry.Version {
		ch <- m.NewResult(proto.Marshal(&ReplaceResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		}))
		return
	}

	// If the value was specified and does not match the entry value, fail the replace.
	if len(request.PreviousValue) != 0 && bytes.Equal(request.PreviousValue, oldEntry.Value) {
		ch <- m.NewResult(proto.Marshal(&ReplaceResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		}))
		return
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

	// Update links for previous and next entries
	if newEntry.Prev != nil {
		newEntry.Prev.Next = newEntry
	}
	if newEntry.Next != nil {
		newEntry.Next.Prev = newEntry
	}

	m.entries[request.Key] = newEntry
	m.indexes[newEntry.Index] = newEntry

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

	ch <- m.NewResult(proto.Marshal(&ReplaceResponse{
		Status:          UpdateStatus_OK,
		Index:           newEntry.Index,
		Key:             newEntry.Key,
		PreviousValue:   oldEntry.Value,
		PreviousVersion: oldEntry.Version,
		NewVersion:      newEntry.Version,
		Created:         newEntry.Created,
		Updated:         newEntry.Updated,
	}))
}

// Remove removes a key/value pair from the map
func (m *Service) Remove(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &RemoveRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- m.NewFailure(err)
		return
	}

	var entry *LinkedMapEntryValue
	if request.Index > 0 {
		entry = m.indexes[request.Index]
	} else {
		entry = m.entries[request.Key]
	}

	if entry == nil {
		ch <- m.NewResult(proto.Marshal(&RemoveResponse{
			Status: UpdateStatus_NOOP,
		}))
		return
	}

	// If the request version is set, verify that the request version matches the entry version.
	if request.Version > 0 && request.Version != entry.Version {
		ch <- m.NewResult(proto.Marshal(&RemoveResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		}))
		return
	}

	// Delete the entry from the map.
	delete(m.entries, request.Key)
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
		Key:     request.Key,
		Index:   entry.Index,
		Value:   entry.Value,
		Version: entry.Version,
		Created: entry.Created,
		Updated: entry.Updated,
	})

	ch <- m.NewResult(proto.Marshal(&ReplaceResponse{
		Status:          UpdateStatus_OK,
		Index:           entry.Index,
		Key:             entry.Key,
		PreviousValue:   entry.Value,
		PreviousVersion: entry.Version,
		Created:         entry.Created,
		Updated:         entry.Updated,
	}))
}

// Get gets a value from the map
func (m *Service) Get(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &GetRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- m.NewFailure(err)
		return
	}

	var entry *LinkedMapEntryValue
	var ok bool
	if request.Index > 0 {
		entry, ok = m.indexes[request.Index]
	} else {
		entry, ok = m.entries[request.Key]
	}

	if !ok {
		ch <- m.NewResult(proto.Marshal(&GetResponse{}))
	} else {
		ch <- m.NewResult(proto.Marshal(&GetResponse{
			Index:   entry.Index,
			Key:     entry.Key,
			Value:   entry.Value,
			Version: entry.Version,
			Created: entry.Created,
			Updated: entry.Updated,
		}))
	}
}

// ContainsKey checks if the map contains a key
func (m *Service) ContainsKey(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &ContainsKeyRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- m.NewFailure(err)
		return
	}

	_, ok := m.entries[request.Key]
	ch <- m.NewResult(proto.Marshal(&ContainsKeyResponse{
		ContainsKey: ok,
	}))
}

// Size returns the size of the map
func (m *Service) Size(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	ch <- m.NewResult(proto.Marshal(&SizeResponse{
		Size_: int32(len(m.entries)),
	}))
}

// Clear removes all entries from the map
func (m *Service) Clear(value []byte, ch chan<- service.Result) {
	defer close(ch)
	m.entries = make(map[string]*LinkedMapEntryValue)
	m.indexes = make(map[uint64]*LinkedMapEntryValue)
	ch <- m.NewResult(proto.Marshal(&ClearResponse{}))
}

// Events sends change events to the client
func (m *Service) Events(bytes []byte, ch chan<- service.Result) {
	request := &ListenRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- m.NewFailure(err)
		close(ch)
	}

	// Send an OPEN response to notify the client the stream is open
	ch <- m.NewResult(proto.Marshal(&ListenResponse{
		Type: ListenResponse_OPEN,
	}))

	if request.Replay {
		entry := m.firstEntry
		for entry != nil {
			ch <- m.NewResult(proto.Marshal(&ListenResponse{
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
func (m *Service) Entries(value []byte, ch chan<- service.Result) {
	defer close(ch)
	entry := m.firstEntry
	for entry != nil {
		ch <- m.NewResult(proto.Marshal(&EntriesResponse{
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
	for _, session := range m.Sessions() {
		for _, ch := range session.ChannelsOf("events") {
			ch <- m.NewSuccess(bytes)
		}
	}
}
