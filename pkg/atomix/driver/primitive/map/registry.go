package _map

import (
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
)

// NewMapProxyRegistry creates a new MapProxyRegistry
func NewMapProxyRegistry() *MapProxyRegistry {
	return &MapProxyRegistry{
		proxies: make(map[primitiveapi.PrimitiveId]_map.MapServiceServer),
	}
}

type MapProxyRegistry struct {
	proxies map[primitiveapi.PrimitiveId]_map.MapServiceServer
	mu      sync.RWMutex
}

func (r *MapProxyRegistry) AddProxy(id primitiveapi.PrimitiveId, server _map.MapServiceServer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; ok {
		return errors.NewAlreadyExists("proxy '%s' already exists", id)
	}
	r.proxies[id] = server
	return nil
}

func (r *MapProxyRegistry) RemoveProxy(id primitiveapi.PrimitiveId) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; !ok {
		return errors.NewNotFound("proxy '%s' not found", id)
	}
	delete(r.proxies, id)
	return nil
}

func (r *MapProxyRegistry) GetProxy(id primitiveapi.PrimitiveId) (_map.MapServiceServer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	proxy, ok := r.proxies[id]
	if !ok {
		return nil, errors.NewNotFound("proxy '%s' not found", id)
	}
	return proxy, nil
}
