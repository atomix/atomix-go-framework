
package log

import (
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
	log "github.com/atomix/api/go/atomix/primitive/log"
)

// NewLogProxyRegistry creates a new LogProxyRegistry
func NewLogProxyRegistry() *LogProxyRegistry {
	return &LogProxyRegistry{
		proxies: make(map[primitiveapi.PrimitiveId]log.LogServiceServer),
	}
}

type LogProxyRegistry struct {
	proxies map[primitiveapi.PrimitiveId]log.LogServiceServer
	mu      sync.RWMutex
}

func (r *LogProxyRegistry) AddProxy(id primitiveapi.PrimitiveId, server log.LogServiceServer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; ok {
		return errors.NewAlreadyExists("proxy '%s' already exists", id)
	}
	r.proxies[id] = server
	return nil
}

func (r *LogProxyRegistry) RemoveProxy(id primitiveapi.PrimitiveId) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; !ok {
		return errors.NewNotFound("proxy '%s' not found", id)
	}
	delete(r.proxies, id)
	return nil
}

func (r *LogProxyRegistry) GetProxy(id primitiveapi.PrimitiveId) (log.LogServiceServer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	proxy, ok := r.proxies[id]
	if !ok {
		return nil, errors.NewNotFound("proxy '%s' not found", id)
	}
	return proxy, nil
}
