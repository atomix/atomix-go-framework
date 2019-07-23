package _map

import (
	"bytes"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/golang/protobuf/proto"
	"time"
)

// RegisterMapService registers the map service in the given service registry
func RegisterMapService(registry *service.ServiceRegistry) {
	registry.Register("map", newMapService)
}

// newMapService returns a new MapService
func newMapService(context service.Context) service.Service {
	service := &MapService{
		SessionizedService: service.NewSessionizedService(context),
		entries:            make(map[string]*MapEntryValue),
		timers:             make(map[string]service.Timer),
	}
	service.init()
	return service
}

// MapService is a state machine for a map primitive
type MapService struct {
	*service.SessionizedService
	entries map[string]*MapEntryValue
	timers  map[string]service.Timer
}

// init initializes the map service
func (m *MapService) init() {
	m.Executor.Register("put", m.Put)
	m.Executor.Register("get", m.Get)
}

// Backup backs up the map service
func (m *MapService) Backup() ([]byte, error) {
	snapshot := &MapSnapshot{
		Entries: m.entries,
	}
	return proto.Marshal(snapshot)
}

// Restore restores the map service
func (m *MapService) Restore(bytes []byte) error {
	snapshot := &MapSnapshot{}
	if err := proto.Unmarshal(bytes, snapshot); err != nil {
		return err
	}
	m.entries = snapshot.Entries
	return nil
}

// Put puts a key/value pair in the map
func (m *MapService) Put(value []byte, ch chan<- service.Result) {
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
			Ttl:     request.Ttl * int64(time.Millisecond),
			Created: m.Context.Timestamp().UnixNano(),
		}
		m.entries[request.Key] = newValue

		// Schedule the timeout for the value if necessary.
		m.scheduleTtl(request.Key, newValue)

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
	} else {
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
		Ttl:     request.Ttl * int64(time.Millisecond),
		Created: m.Context.Timestamp().UnixNano(),
	}
	m.entries[request.Key] = newValue

	// Schedule the timeout for the value if necessary.
	m.scheduleTtl(request.Key, newValue)

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

// Get gets a value from the map
func (m *MapService) Get(bytes []byte, ch chan<- service.Result) {
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

func (m *MapService) scheduleTtl(key string, value *MapEntryValue) {
	m.cancelTtl(key)
	if value.Ttl > 0 {
		m.timers[key] = m.Scheduler.ScheduleOnce(time.Duration(value.Ttl-(m.Context.Timestamp().UnixNano()-value.Created)), func() {
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

func (m *MapService) cancelTtl(key string) {
	timer, ok := m.timers[key]
	if ok {
		timer.Cancel()
	}
}

func (m *MapService) sendEvent(event *ListenResponse) {
	bytes, _ := proto.Marshal(event)
	for _, session := range m.Sessions() {
		for _, ch := range session.Channels() {
			ch <- m.NewSuccess(bytes)
		}
	}
}

// mapValue is a versioned map value
type mapValue struct {
	value   []byte
	version uint64
	created int64
	ttl     int64
}
