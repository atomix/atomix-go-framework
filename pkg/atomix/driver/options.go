// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package driver

import "github.com/atomix/atomix-go-framework/pkg/atomix/driver/env"

type driverOptions struct {
	env.DriverEnv
}

func applyOptions(opts ...Option) driverOptions {
	options := driverOptions{
		DriverEnv: env.GetDriverEnv(),
	}
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

// Option is a driver option
type Option func(opts *driverOptions)

// WithNamespace sets the pod namespace
func WithNamespace(namespace string) Option {
	return func(opts *driverOptions) {
		opts.Namespace = namespace
	}
}

// WithName sets the pod name
func WithName(name string) Option {
	return func(opts *driverOptions) {
		opts.Name = name
	}
}

// WithNode sets the pod node
func WithNode(node string) Option {
	return func(opts *driverOptions) {
		opts.Node = node
	}
}
