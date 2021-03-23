
package counter

import (
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
)

// NewCounterProxyRegistry creates a new CounterProxyRegistry
func NewCounterProxyRegistry() *CounterProxyRegistry {
	return &CounterProxyRegistry{
		proxies: make(map[primitiveapi.PrimitiveId]counter.CounterServiceServer),
	}
}

type CounterProxyRegistry struct {
	proxies map[primitiveapi.PrimitiveId]counter.CounterServiceServer
	mu      sync.RWMutex
}

func (r *CounterProxyRegistry) AddProxy(id primitiveapi.PrimitiveId, server counter.CounterServiceServer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; ok {
		return errors.NewAlreadyExists("proxy '%s' already exists", id)
	}
	r.proxies[id] = server
	return nil
}

func (r *CounterProxyRegistry) RemoveProxy(id primitiveapi.PrimitiveId) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; !ok {
		return errors.NewNotFound("proxy '%s' not found", id)
	}
	delete(r.proxies, id)
	return nil
}

func (r *CounterProxyRegistry) GetProxy(id primitiveapi.PrimitiveId) (counter.CounterServiceServer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	proxy, ok := r.proxies[id]
	if !ok {
		return nil, errors.NewNotFound("proxy '%s' not found", id)
	}
	return proxy, nil
}
