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
	"github.com/atomix/atomix-go-node/pkg/atomix/stream"
	"github.com/atomix/atomix-go-node/pkg/atomix/util"
	"github.com/golang/protobuf/proto"
	"io"
)

func init() {
	node.RegisterService(mapType, newService)
}

// newService returns a new Service
func newService(context service.Context) service.Service {
	service := &Service{
		SessionizedService: service.NewSessionizedService(context),
		entries:            make(map[string]*MapEntryValue),
		timers:             make(map[string]service.Timer),
		listeners:          make(map[uint64]map[uint64]listener),
	}
	service.init()
	return service
}

// Service is a state machine for a map primitive
type Service struct {
	*service.SessionizedService
	entries   map[string]*MapEntryValue
	timers    map[string]service.Timer
	listeners map[uint64]map[uint64]listener
}

// init initializes the map service
func (m *Service) init() {
	m.Executor.RegisterUnaryOperation(opPut, m.Put)
	m.Executor.RegisterUnaryOperation(opReplace, m.Replace)
	m.Executor.RegisterUnaryOperation(opRemove, m.Remove)
	m.Executor.RegisterUnaryOperation(opGet, m.Get)
	m.Executor.RegisterUnaryOperation(opExists, m.Exists)
	m.Executor.RegisterUnaryOperation(opSize, m.Size)
	m.Executor.RegisterUnaryOperation(opClear, m.Clear)
	m.Executor.RegisterStreamOperation(opEvents, m.Events)
	m.Executor.RegisterStreamOperation(opEntries, m.Entries)
}

// Snapshot takes a snapshot of the service
func (m *Service) Snapshot(writer io.Writer) error {
	if err := m.SessionizedService.Snapshot(writer); err != nil {
		return err
	}

	listeners := make([]*Listener, 0)
	for sessionID, sessionListeners := range m.listeners {
		for streamID, sessionListener := range sessionListeners {
			listeners = append(listeners, &Listener{
				SessionId: sessionID,
				StreamId:  streamID,
				Key:       sessionListener.key,
			})
		}
	}

	if err := util.WriteVarInt(writer, len(listeners)); err != nil {
		return err
	}
	if err := util.WriteSlice(writer, listeners, proto.Marshal); err != nil {
		return err
	}

	return util.WriteMap(writer, m.entries, func(key string, value *MapEntryValue) ([]byte, error) {
		return proto.Marshal(&MapEntry{
			Key:   key,
			Value: value,
		})
	})
}

// Install restores the service from a snapshot
func (m *Service) Install(reader io.Reader) error {
	if err := m.SessionizedService.Install(reader); err != nil {
		return err
	}

	length, err := util.ReadVarInt(reader)
	if err != nil {
		return err
	}

	listeners := make([]*Listener, length)
	err = util.ReadSlice(reader, listeners, func(data []byte) (*Listener, error) {
		listener := &Listener{}
		if err := proto.Unmarshal(data, listener); err != nil {
			return nil, err
		}
		return listener, nil
	})
	if err != nil {
		return err
	}

	m.listeners = make(map[uint64]map[uint64]listener)
	for _, snapshotListener := range listeners {
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

	entries := make(map[string]*MapEntryValue)
	err = util.ReadMap(reader, entries, func(data []byte) (string, *MapEntryValue, error) {
		entry := &MapEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			return "", nil, err
		}
		return entry.Key, entry.Value, nil
	})
	if err != nil {
		return err
	}
	m.entries = entries
	return nil
}

// Put puts a key/value pair in the map
func (m *Service) Put(value []byte) ([]byte, error) {
	request := &PutRequest{}
	if err := proto.Unmarshal(value, request); err != nil {
		return nil, err
	}

	oldValue := m.entries[request.Key]
	if oldValue == nil {
		// If the version is positive then reject the request.
		if !request.IfEmpty && request.Version > 0 {
			return proto.Marshal(&PutResponse{
				Status: UpdateStatus_PRECONDITION_FAILED,
			})
		}

		// Create a new entry value and set it in the map.
		newValue := &MapEntryValue{
			Value:   request.Value,
			Version: m.Context.Index(),
			TTL:     request.TTL,
			Created: m.Context.Timestamp(),
			Updated: m.Context.Timestamp(),
		}
		m.entries[request.Key] = newValue

		// Schedule the timeout for the value if necessary.
		m.scheduleTTL(request.Key, newValue)

		// Publish an event to listener streams.
		m.sendEvent(&ListenResponse{
			Type:    ListenResponse_INSERTED,
			Key:     request.Key,
			Value:   newValue.Value,
			Version: newValue.Version,
			Created: newValue.Created,
			Updated: newValue.Updated,
		})

		return proto.Marshal(&PutResponse{
			Status: UpdateStatus_OK,
		})
	}

	// If the version is -1 then reject the request.
	// If the version is positive then compare the version to the current version.
	if request.IfEmpty || (!request.IfEmpty && request.Version > 0 && request.Version != oldValue.Version) {
		return proto.Marshal(&PutResponse{
			Status:          UpdateStatus_PRECONDITION_FAILED,
			PreviousValue:   oldValue.Value,
			PreviousVersion: oldValue.Version,
		})
	}

	// If the value is equal to the current value, return a no-op.
	if bytes.Equal(oldValue.Value, request.Value) {
		return proto.Marshal(&PutResponse{
			Status:          UpdateStatus_NOOP,
			PreviousValue:   oldValue.Value,
			PreviousVersion: oldValue.Version,
		})
	}

	// Create a new entry value and set it in the map.
	newValue := &MapEntryValue{
		Value:   request.Value,
		Version: m.Context.Index(),
		TTL:     request.TTL,
		Created: oldValue.Created,
		Updated: m.Context.Timestamp(),
	}
	m.entries[request.Key] = newValue

	// Schedule the timeout for the value if necessary.
	m.scheduleTTL(request.Key, newValue)

	// Publish an event to listener streams.
	m.sendEvent(&ListenResponse{
		Type:    ListenResponse_UPDATED,
		Key:     request.Key,
		Value:   newValue.Value,
		Version: newValue.Version,
		Created: newValue.Created,
		Updated: newValue.Updated,
	})

	return proto.Marshal(&PutResponse{
		Status:          UpdateStatus_OK,
		PreviousValue:   oldValue.Value,
		PreviousVersion: oldValue.Version,
		Created:         newValue.Created,
		Updated:         newValue.Updated,
	})
}

// Replace replaces a key/value pair in the map
func (m *Service) Replace(value []byte) ([]byte, error) {
	request := &ReplaceRequest{}
	if err := proto.Unmarshal(value, request); err != nil {
		return nil, err
	}

	oldValue, ok := m.entries[request.Key]
	if !ok {
		return proto.Marshal(&ReplaceResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		})
	}

	// If the version was specified and does not match the entry version, fail the replace.
	if request.PreviousVersion != 0 && request.PreviousVersion != oldValue.Version {
		return proto.Marshal(&ReplaceResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		})
	}

	// If the value was specified and does not match the entry value, fail the replace.
	if len(request.PreviousValue) != 0 && bytes.Equal(request.PreviousValue, oldValue.Value) {
		return proto.Marshal(&ReplaceResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		})
	}

	// If we've made it this far, update the entry.
	// Create a new entry value and set it in the map.
	newValue := &MapEntryValue{
		Value:   request.NewValue,
		Version: m.Context.Index(),
		TTL:     request.TTL,
		Created: oldValue.Created,
		Updated: m.Context.Timestamp(),
	}
	m.entries[request.Key] = newValue

	// Schedule the timeout for the value if necessary.
	m.scheduleTTL(request.Key, newValue)

	// Publish an event to listener streams.
	m.sendEvent(&ListenResponse{
		Type:    ListenResponse_UPDATED,
		Key:     request.Key,
		Value:   newValue.Value,
		Version: newValue.Version,
		Created: newValue.Created,
		Updated: newValue.Updated,
	})

	return proto.Marshal(&ReplaceResponse{
		Status:          UpdateStatus_OK,
		PreviousValue:   oldValue.Value,
		PreviousVersion: oldValue.Version,
		NewVersion:      newValue.Version,
		Created:         newValue.Created,
		Updated:         newValue.Updated,
	})
}

// Remove removes a key/value pair from the map
func (m *Service) Remove(bytes []byte) ([]byte, error) {
	request := &RemoveRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	value, ok := m.entries[request.Key]
	if !ok {
		return proto.Marshal(&RemoveResponse{
			Status: UpdateStatus_NOOP,
		})
	}

	// If the request version is set, verify that the request version matches the entry version.
	if request.Version > 0 && request.Version != value.Version {
		return proto.Marshal(&RemoveResponse{
			Status: UpdateStatus_PRECONDITION_FAILED,
		})
	}

	// Delete the entry from the map.
	delete(m.entries, request.Key)

	// Cancel any TTLs.
	m.cancelTTL(request.Key)

	// Publish an event to listener streams.
	m.sendEvent(&ListenResponse{
		Type:    ListenResponse_REMOVED,
		Key:     request.Key,
		Value:   value.Value,
		Version: value.Version,
		Created: value.Created,
		Updated: value.Updated,
	})

	return proto.Marshal(&ReplaceResponse{
		Status:          UpdateStatus_OK,
		PreviousValue:   value.Value,
		PreviousVersion: value.Version,
		Created:         value.Created,
		Updated:         value.Updated,
	})
}

// Get gets a value from the map
func (m *Service) Get(bytes []byte) ([]byte, error) {
	request := &GetRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	value, ok := m.entries[request.Key]
	if !ok {
		return proto.Marshal(&GetResponse{})
	}
	return proto.Marshal(&GetResponse{
		Value:   value.Value,
		Version: value.Version,
		Created: value.Created,
		Updated: value.Updated,
	})
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
	m.entries = make(map[string]*MapEntryValue)
	return proto.Marshal(&ClearResponse{})
}

// Events sends change events to the client
func (m *Service) Events(bytes []byte, stream stream.WriteStream) {
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
		stream: stream,
	}
	listeners, ok := m.listeners[m.Session().ID]
	if !ok {
		listeners = make(map[uint64]listener)
		m.listeners[m.Session().ID] = listeners
	}
	listeners[m.Session().StreamID()] = l

	// If replay was requested, send existing entries
	if request.Replay {
		for key, value := range m.entries {
			stream.Result(proto.Marshal(&ListenResponse{
				Type:    ListenResponse_NONE,
				Key:     key,
				Value:   value.Value,
				Version: value.Version,
				Created: value.Created,
				Updated: value.Updated,
			}))
		}
	}
}

// Entries returns a stream of entries to the client
func (m *Service) Entries(value []byte, stream stream.WriteStream) {
	defer stream.Close()
	for key, entry := range m.entries {
		stream.Result(proto.Marshal(&EntriesResponse{
			Key:     key,
			Value:   entry.Value,
			Version: entry.Version,
			Created: entry.Created,
			Updated: entry.Updated,
		}))
	}
}

func (m *Service) scheduleTTL(key string, value *MapEntryValue) {
	m.cancelTTL(key)
	if value.TTL != nil {
		m.timers[key] = m.Scheduler.ScheduleOnce(value.Created.Add(*value.TTL).Sub(m.Context.Timestamp()), func() {
			delete(m.entries, key)
			m.sendEvent(&ListenResponse{
				Type:    ListenResponse_REMOVED,
				Key:     key,
				Value:   value.Value,
				Version: uint64(value.Version),
				Created: value.Created,
				Updated: value.Updated,
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
				} else {
					listener.stream.Value(bytes)
				}
			}
		}
	}
}

type listener struct {
	key    string
	stream stream.WriteStream
}
