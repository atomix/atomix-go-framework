// Code generated by atomix-go-framework. DO NOT EDIT.
package _map

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	_map "github.com/atomix/atomix-api/go/atomix/primitive/map"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"sync"
)

// NewProxyRegistry creates a new ProxyRegistry
func NewProxyRegistry() *ProxyRegistry {
	return &ProxyRegistry{
		proxies: make(map[primitiveapi.PrimitiveId]_map.MapServiceServer),
	}
}

type ProxyRegistry struct {
	proxies map[driverapi.ProxyId]_map.MapServiceServer
	mu      sync.RWMutex
}

func (r *ProxyRegistry) AddProxy(id driverapi.ProxyId, server _map.MapServiceServer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; ok {
		return errors.NewAlreadyExists("proxy '%s' already exists", id)
	}
	r.proxies[id] = server
	log.Debugf("Added proxy %s to registry", id)
	return nil
}

func (r *ProxyRegistry) RemoveProxy(id driverapi.ProxyId) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; !ok {
		return errors.NewNotFound("proxy '%s' not found", id)
	}
	delete(r.proxies, id)
	log.Debugf("Removed proxy %s from registry", id)
	return nil
}

func (r *ProxyRegistry) GetProxy(id driverapi.ProxyId) (_map.MapServiceServer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	proxy, ok := r.proxies[id]
	if !ok {
		return nil, errors.NewNotFound("proxy '%s' not found", id)
	}
	return proxy, nil
}
