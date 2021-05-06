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

package broker

import "os"

const (
	namespaceEnv = "ATOMIX_BROKER_NAMESPACE"
	nameEnv      = "ATOMIX_BROKER_NAME"
	nodeEnv      = "ATOMIX_BROKER_NODE"
)

const (
	defaultID   = "atomix-broker"
	defaultHost = ""
	defaultPort = 5678
)

type brokerOptions struct {
	namespace string
	name      string
	node      string
	id        string
	host      string
	port      int
}

func applyOptions(opts ...Option) brokerOptions {
	options := brokerOptions{
		namespace: os.Getenv(namespaceEnv),
		name:      os.Getenv(nameEnv),
		node:      os.Getenv(nodeEnv),
		id:        defaultID,
		host:      defaultHost,
		port:      defaultPort,
	}
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

// Option is a broker option
type Option func(opts *brokerOptions)

// WithNamespace sets the pod namespace
func WithNamespace(namespace string) Option {
	return func(opts *brokerOptions) {
		opts.namespace = namespace
	}
}

// WithName sets the pod name
func WithName(name string) Option {
	return func(opts *brokerOptions) {
		opts.name = name
	}
}

// WithNode sets the pod node
func WithNode(node string) Option {
	return func(opts *brokerOptions) {
		opts.node = node
	}
}

// WithID sets the broker identifier
func WithID(id string) Option {
	return func(opts *brokerOptions) {
		opts.id = id
	}
}

// WithHost sets the host to which the broker should bind
func WithHost(host string) Option {
	return func(opts *brokerOptions) {
		opts.host = host
	}
}

// WithPort sets the port to which the broker should bind
func WithPort(port int) Option {
	return func(opts *brokerOptions) {
		opts.port = port
	}
}
