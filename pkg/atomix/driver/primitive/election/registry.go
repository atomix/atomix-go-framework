package election

import (
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	election "github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
)

// NewElectionProxyRegistry creates a new ElectionProxyRegistry
func NewElectionProxyRegistry() *ElectionProxyRegistry {
	return &ElectionProxyRegistry{
		proxies: make(map[primitiveapi.PrimitiveId]election.LeaderElectionServiceServer),
	}
}

type ElectionProxyRegistry struct {
	proxies map[primitiveapi.PrimitiveId]election.LeaderElectionServiceServer
	mu      sync.RWMutex
}

func (r *ElectionProxyRegistry) AddProxy(id primitiveapi.PrimitiveId, server election.LeaderElectionServiceServer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; ok {
		return errors.NewAlreadyExists("proxy '%s' already exists", id)
	}
	r.proxies[id] = server
	return nil
}

func (r *ElectionProxyRegistry) RemoveProxy(id primitiveapi.PrimitiveId) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; !ok {
		return errors.NewNotFound("proxy '%s' not found", id)
	}
	delete(r.proxies, id)
	return nil
}

func (r *ElectionProxyRegistry) GetProxy(id primitiveapi.PrimitiveId) (election.LeaderElectionServiceServer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	proxy, ok := r.proxies[id]
	if !ok {
		return nil, errors.NewNotFound("proxy '%s' not found", id)
	}
	return proxy, nil
}
