package set

import (
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	set "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
)

// NewSetProxyRegistry creates a new SetProxyRegistry
func NewSetProxyRegistry() *SetProxyRegistry {
	return &SetProxyRegistry{
		proxies: make(map[primitiveapi.PrimitiveId]set.SetServiceServer),
	}
}

type SetProxyRegistry struct {
	proxies map[primitiveapi.PrimitiveId]set.SetServiceServer
	mu      sync.RWMutex
}

func (r *SetProxyRegistry) AddProxy(id primitiveapi.PrimitiveId, server set.SetServiceServer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; ok {
		return errors.NewAlreadyExists("proxy '%s' already exists", id)
	}
	r.proxies[id] = server
	return nil
}

func (r *SetProxyRegistry) RemoveProxy(id primitiveapi.PrimitiveId) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; !ok {
		return errors.NewNotFound("proxy '%s' not found", id)
	}
	delete(r.proxies, id)
	return nil
}

func (r *SetProxyRegistry) GetProxy(id primitiveapi.PrimitiveId) (set.SetServiceServer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	proxy, ok := r.proxies[id]
	if !ok {
		return nil, errors.NewNotFound("proxy '%s' not found", id)
	}
	return proxy, nil
}
