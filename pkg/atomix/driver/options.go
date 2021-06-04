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
