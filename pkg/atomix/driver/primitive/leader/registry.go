package leader

import (
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	leader "github.com/atomix/api/go/atomix/primitive/leader"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
)

// NewLeaderLatchProxyRegistry creates a new LeaderLatchProxyRegistry
func NewLeaderLatchProxyRegistry() *LeaderLatchProxyRegistry {
	return &LeaderLatchProxyRegistry{
		proxies: make(map[primitiveapi.PrimitiveId]leader.LeaderLatchServiceServer),
	}
}

type LeaderLatchProxyRegistry struct {
	proxies map[primitiveapi.PrimitiveId]leader.LeaderLatchServiceServer
	mu      sync.RWMutex
}

func (r *LeaderLatchProxyRegistry) AddProxy(id primitiveapi.PrimitiveId, server leader.LeaderLatchServiceServer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; ok {
		return errors.NewAlreadyExists("proxy '%s' already exists", id)
	}
	r.proxies[id] = server
	return nil
}

func (r *LeaderLatchProxyRegistry) RemoveProxy(id primitiveapi.PrimitiveId) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; !ok {
		return errors.NewNotFound("proxy '%s' not found", id)
	}
	delete(r.proxies, id)
	return nil
}

func (r *LeaderLatchProxyRegistry) GetProxy(id primitiveapi.PrimitiveId) (leader.LeaderLatchServiceServer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	proxy, ok := r.proxies[id]
	if !ok {
		return nil, errors.NewNotFound("proxy '%s' not found", id)
	}
	return proxy, nil
}
