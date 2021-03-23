package list

import (
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	list "github.com/atomix/api/go/atomix/primitive/list"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
)

// NewListProxyRegistry creates a new ListProxyRegistry
func NewListProxyRegistry() *ListProxyRegistry {
	return &ListProxyRegistry{
		proxies: make(map[primitiveapi.PrimitiveId]list.ListServiceServer),
	}
}

type ListProxyRegistry struct {
	proxies map[primitiveapi.PrimitiveId]list.ListServiceServer
	mu      sync.RWMutex
}

func (r *ListProxyRegistry) AddProxy(id primitiveapi.PrimitiveId, server list.ListServiceServer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; ok {
		return errors.NewAlreadyExists("proxy '%s' already exists", id)
	}
	r.proxies[id] = server
	return nil
}

func (r *ListProxyRegistry) RemoveProxy(id primitiveapi.PrimitiveId) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; !ok {
		return errors.NewNotFound("proxy '%s' not found", id)
	}
	delete(r.proxies, id)
	return nil
}

func (r *ListProxyRegistry) GetProxy(id primitiveapi.PrimitiveId) (list.ListServiceServer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	proxy, ok := r.proxies[id]
	if !ok {
		return nil, errors.NewNotFound("proxy '%s' not found", id)
	}
	return proxy, nil
}
