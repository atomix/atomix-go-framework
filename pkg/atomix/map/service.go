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
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/golang/protobuf/proto"
)

func init() {
	node.RegisterService("map", newService)
}

// newService returns a new Service
func newService(context service.Context) service.Service {
	service := &Service{
		SessionizedService: service.NewSessionizedService(context),
		entries:            make(map[string]*MapEntryValue),
		timers:             make(map[string]service.Timer),
	}
	service.init()
	return service
}

// Service is a state machine for a map primitive
type Service struct {
	*service.SessionizedService
	entries map[string]*MapEntryValue
	timers  map[string]service.Timer
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

// Backup backs up the map service
func (m *Service) Backup() ([]byte, error) {
	snapshot := &MapSnapshot{
		Entries: m.entries,
	}
	return proto.Marshal(snapshot)
}

// Restore restores the map service
func (m *Service) Restore(bytes []byte) error {
	snapshot := &MapSnapshot{}
	if err := proto.Unmarshal(bytes, snapshot); err != nil {
		return err
	}
	m.entries = snapshot.Entries
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

	oldValue := m.entries[request.Key]
	if oldValue == nil {
		// If the version is positive then reject the request.
		if request.Version > 0 {
			ch <- m.NewResult(proto.Marshal(&PutResponse{
				Status: UpdateStatus_PRECONDITION_FAILED,
			}))
			return
		}

		// Create a new entry value and set it in the map.
		newValue := &MapEntryValue{
			Value:   request.Value,
			Version: m.Context.Index(),
			TTL:     request.TTL,
			Created: m.Context.Timestamp(),
		}
		m.entries[request.Key] = newValue

		// Schedule the timeout for the value if necessary.
		m.scheduleTTL(request.Key, newValue)

		// Publish an event to listener streams.
		m.sendEvent(&ListenResponse{
			Type:       ListenResponse_INSERTED,
			Key:        request.Key,
			NewValue:   newValue.Value,
			NewVersion: newValue.Version,
		})

		ch <- m.NewResult(proto.Marshal(&PutResponse{
			Status: UpdateStatus_OK,
		}))
		return
	}

	// If the version is -1 then reject the request.
	// If the version is positive then compare the version to the current version.
	if request.IfEmpty || (request.Version > 0 && request.Version != oldValue.Version) {
		ch <- m.NewResult(proto.Marshal(&PutResponse{
			Status:          UpdateStatus_PRECONDITION_FAILED,
			PreviousValue:   oldValue.Value,
			PreviousVersion: oldValue.Version,
		}))
		return
	}

	// If the value is equal to the current value, return a no-op.
	if bytes.Equal(oldValue.Value, request.Value) {
		ch <- m.NewResult(proto.Marshal(&PutResponse{
			Status:          UpdateStatus_NOOP,
			PreviousValue:   oldValue.Value,
			PreviousVersion: oldValue.Version,
		}))
		return
	}

	// Create a new entry value and set it in the map.
	newValue := &MapEntryValue{
		Value:   request.Value,
		Version: m.Context.Index(),
		TTL:     request.TTL,
		Created: m.Context.Timestamp(),
	}
	m.entries[request.Key] = newValue

	// Schedule the timeout for the value if necessary.
	m.scheduleTTL(request.Key, newValue)

	// Publish an event to listener streams.
	m.sendEvent(&ListenResponse{
		Type:       ListenResponse_UPDATED,
		Key:        request.Key,
		OldValue:   oldValue.Value,
		OldVersion: oldValue.Version,
		NewValue:   newValue.Value,
		NewVersion: newValue.Version,
	})

	ch <- m.NewResult(proto.Marshal(&PutResponse{
		Status:          UpdateStatus_OK,
		PreviousValue:   oldValue.Value,
		PreviousVersion: oldValue.Version,
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

	oldValue, ok := m.entries[request.Key]
	if !ok {
		ch <- m.NewResult(proto.Marshal(&ReplaceResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		}))
		return
	}

	// If the version was specified and does not match the entry version, fail the replace.
	if request.PreviousVersion != 0 && request.PreviousVersion != oldValue.Version {
		ch <- m.NewResult(proto.Marshal(&ReplaceResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		}))
		return
	}

	// If the value was specified and does not match the entry value, fail the replace.
	if len(request.PreviousValue) != 0 && bytes.Equal(request.PreviousValue, oldValue.Value) {
		ch <- m.NewResult(proto.Marshal(&ReplaceResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		}))
		return
	}

	// If we've made it this far, update the entry.
	// Create a new entry value and set it in the map.
	newValue := &MapEntryValue{
		Value:   request.NewValue,
		Version: m.Context.Index(),
		TTL:     request.TTL,
		Created: m.Context.Timestamp(),
	}
	m.entries[request.Key] = newValue

	// Schedule the timeout for the value if necessary.
	m.scheduleTTL(request.Key, newValue)

	// Publish an event to listener streams.
	m.sendEvent(&ListenResponse{
		Type:       ListenResponse_UPDATED,
		Key:        request.Key,
		OldValue:   oldValue.Value,
		OldVersion: oldValue.Version,
		NewValue:   newValue.Value,
		NewVersion: newValue.Version,
	})

	ch <- m.NewResult(proto.Marshal(&ReplaceResponse{
		Status:          UpdateStatus_OK,
		PreviousValue:   oldValue.Value,
		PreviousVersion: oldValue.Version,
		NewVersion:      newValue.Version,
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

	value, ok := m.entries[request.Key]
	if !ok {
		ch <- m.NewResult(proto.Marshal(&RemoveResponse{
			Status: UpdateStatus_NOOP,
		}))
		return
	}

	// If the request version is set, verify that the request version matches the entry version.
	if request.Version > 0 && request.Version != value.Version {
		ch <- m.NewResult(proto.Marshal(&RemoveResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		}))
		return
	}

	// Delete the entry from the map.
	delete(m.entries, request.Key)

	// Cancel any TTLs.
	m.cancelTTL(request.Key)

	// Publish an event to listener streams.
	m.sendEvent(&ListenResponse{
		Type:       ListenResponse_REMOVED,
		Key:        request.Key,
		OldValue:   value.Value,
		OldVersion: value.Version,
	})

	ch <- m.NewResult(proto.Marshal(&ReplaceResponse{
		Status:          UpdateStatus_OK,
		PreviousValue:   value.Value,
		PreviousVersion: value.Version,
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

	value, ok := m.entries[request.Key]
	if !ok {
		ch <- m.NewResult(proto.Marshal(&GetResponse{}))
	} else {
		ch <- m.NewResult(proto.Marshal(&GetResponse{
			Value:   value.Value,
			Version: value.Version,
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
	m.entries = make(map[string]*MapEntryValue)
	ch <- m.NewResult(proto.Marshal(&ClearResponse{}))
}

// Events sends change events to the client
func (m *Service) Events(bytes []byte, ch chan<- service.Result) {
	request := &ListenRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- m.NewFailure(err)
		close(ch)
	}

	if request.Replay {
		for key, value := range m.entries {
			ch <- m.NewResult(proto.Marshal(&ListenResponse{
				Type:       ListenResponse_NONE,
				Key:        key,
				NewValue:   value.Value,
				NewVersion: value.Version,
			}))
		}
	}
}

// Entries returns a stream of entries to the client
func (m *Service) Entries(value []byte, ch chan<- service.Result) {
	defer close(ch)
	for key, entry := range m.entries {
		ch <- m.NewResult(proto.Marshal(&EntriesResponse{
			Key:     key,
			Value:   entry.Value,
			Version: entry.Version,
		}))
	}
}

func (m *Service) scheduleTTL(key string, value *MapEntryValue) {
	m.cancelTTL(key)
	if value.TTL != nil {
		m.timers[key] = m.Scheduler.ScheduleOnce(value.Created.Add(*value.TTL).Sub(m.Context.Timestamp()), func() {
			delete(m.entries, key)
			m.sendEvent(&ListenResponse{
				Type:       ListenResponse_REMOVED,
				Key:        key,
				OldValue:   value.Value,
				OldVersion: uint64(value.Version),
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
