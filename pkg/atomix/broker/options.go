// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package broker

import "os"

const (
	namespaceEnv = "ATOMIX_BROKER_NAMESPACE"
	nameEnv      = "ATOMIX_BROKER_NAME"
	nodeEnv      = "ATOMIX_BROKER_NODE"
)

type brokerOptions struct {
	namespace string
	name      string
	node      string
}

func applyOptions(opts ...Option) brokerOptions {
	options := brokerOptions{
		namespace: os.Getenv(namespaceEnv),
		name:      os.Getenv(nameEnv),
		node:      os.Getenv(nodeEnv),
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
