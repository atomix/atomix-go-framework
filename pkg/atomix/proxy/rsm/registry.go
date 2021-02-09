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

package rsm

import (
	"google.golang.org/grpc"
)

// RegisterProxyFunc is a function for registering a proxy server
type RegisterProxyFunc func(server *grpc.Server, client *Client)

// NewRegistry creates a new primitive registry
func NewRegistry() *Registry {
	return &Registry{
		proxies: make(map[string]RegisterProxyFunc),
	}
}

// Registry is the default primitive registry
type Registry struct {
	proxies map[string]RegisterProxyFunc
}

func (r *Registry) Register(primitiveType string, primitive RegisterProxyFunc) {
	r.proxies[primitiveType] = primitive
}

func (r *Registry) GetProxies() []RegisterProxyFunc {
	proxies := make([]RegisterProxyFunc, 0, len(r.proxies))
	for _, proxy := range r.proxies {
		proxies = append(proxies, proxy)
	}
	return proxies
}

func (r *Registry) GetProxy(primitiveType string) RegisterProxyFunc {
	return r.proxies[primitiveType]
}
