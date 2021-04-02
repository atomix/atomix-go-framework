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

const (
	defaultID   = "atomix-broker"
	defaultHost = ""
	defaultPort = 5678
)

type brokerOptions struct {
	id              string
	host            string
	port            int
	applicationHost string
	applicationPort int
}

func applyOptions(opts ...Option) brokerOptions {
	options := brokerOptions{
		id:   defaultID,
		host: defaultHost,
		port: defaultPort,
	}
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

// Option is a broker option
type Option func(opts *brokerOptions)

func WithID(id string) Option {
	return func(opts *brokerOptions) {
		opts.id = id
	}
}

func WithControllerHost(host string) Option {
	return func(opts *brokerOptions) {
		opts.host = host
	}
}

func WithControllerPort(port int) Option {
	return func(opts *brokerOptions) {
		opts.port = port
	}
}

func WithApplicationHost(host string) Option {
	return func(opts *brokerOptions) {
		opts.applicationHost = host
	}
}

func WithApplicationPort(port int) Option {
	return func(opts *brokerOptions) {
		opts.applicationPort = port
	}
}
