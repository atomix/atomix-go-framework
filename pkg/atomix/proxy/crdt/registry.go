// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crdt

import (
	"google.golang.org/grpc"
)

// RegisterProxyFunc is a function for registering a proxy
type RegisterProxyFunc func(server *grpc.Server, client *Client)

// Registry is a primitive registry
type Registry interface {
	// RegisterProxy registers a primitive proxy
	RegisterProxy(f RegisterProxyFunc)

	// GetProxies gets a list of primitives
	GetProxies() []RegisterProxyFunc
}

// primitiveRegistry is the default primitive registry
type primitiveRegistry struct {
	proxies []RegisterProxyFunc
}

func (r *primitiveRegistry) RegisterProxy(primitive RegisterProxyFunc) {
	r.proxies = append(r.proxies, primitive)
}

func (r *primitiveRegistry) GetProxies() []RegisterProxyFunc {
	proxies := make([]RegisterProxyFunc, 0, len(r.proxies))
	for _, proxy := range r.proxies {
		proxies = append(proxies, proxy)
	}
	return proxies
}

// NewRegistry creates a new primitive registry
func NewRegistry() Registry {
	return &primitiveRegistry{}
}
