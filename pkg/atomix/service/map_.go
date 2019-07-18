package service

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"time"
)

const versionEmpty = -1

// newMapService returns a new MapService
func newMapService(scheduler Scheduler, executor Executor, ctx Context) Service {
	service := &MapService{
		SessionizedService: &SessionizedService{
			scheduler: scheduler,
			executor:  executor,
			ctx:       ctx,
		},
		entries: make(map[string]*mapValue),
		timers:  make(map[string]Timer),
	}
	executor.Register("put", service.Put)
	executor.Register("get", service.Get)
	return service
}

// MapService is a state machine for a map primitive
type MapService struct {
	*SessionizedService
	entries map[string]*mapValue
	timers  map[string]Timer
}

// Put puts a key/value pair in the map
func (m *MapService) Put(value []byte) ([]byte, error) {
	request := &PutRequest{}
	if err := proto.Unmarshal(value, request); err != nil {
		return nil, err
	}

	oldValue := m.entries[request.Key]
	if oldValue == nil {
		// If the version is positive then reject the request.
		if request.Version > 0 {
			return proto.Marshal(&PutResponse{
				Status: UpdateStatus_PRECONDITION_FAILED,
			})
		}

		// Create a new entry value and set it in the map.
		newValue := &mapValue{
			value:   request.Value,
			version: m.ctx.Index(),
			ttl:     request.Ttl * int64(time.Millisecond),
			created: m.ctx.Timestamp().UnixNano(),
		}
		m.entries[request.Key] = newValue

		// Schedule the timeout for the value if necessary.
		m.scheduleTtl(request.Key, newValue)

		// Publish an event to listener streams.
		m.sendEvent(&ListenResponse{
			Type:       ListenResponse_INSERTED,
			Key:        request.Key,
			NewValue:   newValue.value,
			NewVersion: newValue.version,
		})

		return proto.Marshal(&PutResponse{
			Status: UpdateStatus_OK,
		})
	} else {
		// If the version is -1 then reject the request.
		// If the version is positive then compare the version to the current version.
		if request.Version == versionEmpty || (request.Version > 0 && request.Version != oldValue.version) {
			return proto.Marshal(&PutResponse{
				Status:          UpdateStatus_PRECONDITION_FAILED,
				PreviousValue:   oldValue.value,
				PreviousVersion: oldValue.version,
			})
		}
	}

	// If the value is equal to the current value, return a no-op.
	if bytes.Equal(oldValue.value, request.Value) {
		return proto.Marshal(&PutResponse{
			Status:          UpdateStatus_NOOP,
			PreviousValue:   oldValue.value,
			PreviousVersion: oldValue.version,
		})
	}

	// Create a new entry value and set it in the map.
	newValue := &mapValue{
		value:   request.Value,
		version: m.ctx.Index(),
		ttl:     request.Ttl * int64(time.Millisecond),
		created: m.ctx.Timestamp().UnixNano(),
	}
	m.entries[request.Key] = newValue

	// Schedule the timeout for the value if necessary.
	m.scheduleTtl(request.Key, newValue)

	// Publish an event to listener streams.
	m.sendEvent(&ListenResponse{
		Type:       ListenResponse_UPDATED,
		Key:        request.Key,
		OldValue:   oldValue.value,
		OldVersion: oldValue.version,
		NewValue:   newValue.value,
		NewVersion: newValue.version,
	})

	return proto.Marshal(&PutResponse{
		Status:          UpdateStatus_OK,
		PreviousValue:   oldValue.value,
		PreviousVersion: oldValue.version,
	})
}

// Get gets a value from the map
func (m *MapService) Get(bytes []byte) ([]byte, error) {
	request := &GetRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	value, ok := m.entries[request.Key]
	if !ok {
		return proto.Marshal(&GetResponse{})
	}
	return proto.Marshal(&GetResponse{
		Value:   value.value,
		Version: value.version,
	})
}

func (m *MapService) scheduleTtl(key string, value *mapValue) {
	m.cancelTtl(key)
	if value.ttl > 0 {
		m.timers[key] = m.scheduler.ScheduleOnce(time.Duration(value.ttl-(m.ctx.Timestamp().UnixNano()-value.created)), func() {
			delete(m.entries, key)
			m.sendEvent(&ListenResponse{
				Type:       ListenResponse_REMOVED,
				Key:        key,
				OldValue:   value.value,
				OldVersion: value.version,
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
		for _, stream := range session.Streams() {
			stream.Next(bytes)
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

func init() {
	registry.Register("map", newMapService)
}
