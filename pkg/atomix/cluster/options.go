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

package cluster

import (
	"google.golang.org/grpc"
)

func applyOptions(opts ...Option) *options {
	options := &options{
		peerPort: 8080,
	}
	for _, opt := range opts {
		opt.apply(options)
	}
	return options
}

type options struct {
	memberID string
	nodeID   string
	peerHost string
	peerPort int
}

// Option provides a peer option
type Option interface {
	apply(options *options)
}

// WithMemberID configures the peer's member ID
func WithMemberID(memberID string) Option {
	return &memberIDOption{id: memberID}
}

type memberIDOption struct {
	id string
}

func (o *memberIDOption) apply(options *options) {
	options.memberID = o.id
	if options.peerHost == "" {
		options.peerHost = o.id
	}
}

// WithNodeID configures the peer's node ID
func WithNodeID(nodeID string) Option {
	return &nodeIDOption{id: nodeID}
}

type nodeIDOption struct {
	id string
}

func (o *nodeIDOption) apply(options *options) {
	options.nodeID = o.id
}

// WithHost configures the peer's host
func WithHost(host string) Option {
	return &hostOption{host: host}
}

type hostOption struct {
	host string
}

func (o *hostOption) apply(options *options) {
	options.peerHost = o.host
}

// WithPort configures the peer's port
func WithPort(port int) Option {
	return &portOption{port: port}
}

type portOption struct {
	port int
}

func (o *portOption) apply(options *options) {
	options.peerPort = o.port
}

func applyServeOptions(opts ...ServeOption) *serveOptions {
	options := &serveOptions{
		services:    make([]Service, 0),
		grpcOptions: make([]grpc.ServerOption, 0),
	}
	for _, opt := range opts {
		opt.apply(options)
	}
	return options
}

type serveOptions struct {
	services    []Service
	grpcOptions []grpc.ServerOption
}

// ServeOption provides a member serve option
type ServeOption interface {
	apply(options *serveOptions)
}

// WithService configures a peer-to-peer service
func WithService(service Service) ServeOption {
	return &serviceOption{
		service: service,
	}
}

type serviceOption struct {
	service Service
}

func (o *serviceOption) apply(options *serveOptions) {
	options.services = append(options.services, o.service)
}

// WithServices configures peer-to-peer services
func WithServices(services ...Service) ServeOption {
	return &servicesOption{
		services: services,
	}
}

type servicesOption struct {
	services []Service
}

func (o *servicesOption) apply(options *serveOptions) {
	options.services = append(options.services, o.services...)
}

// WithServerOption configures a server option
func WithServerOption(option grpc.ServerOption) ServeOption {
	return &serverOptionOption{
		option: option,
	}
}

type serverOptionOption struct {
	option grpc.ServerOption
}

func (o *serverOptionOption) apply(options *serveOptions) {
	options.grpcOptions = append(options.grpcOptions, o.option)
}

// WithServerOptions configures server options
func WithServerOptions(options ...grpc.ServerOption) ServeOption {
	return &serverOptionsOption{
		options: options,
	}
}

type serverOptionsOption struct {
	options []grpc.ServerOption
}

func (o *serverOptionsOption) apply(options *serveOptions) {
	options.grpcOptions = append(options.grpcOptions, o.options...)
}

func applyConnectOptions(opts ...ConnectOption) *connectOptions {
	options := &connectOptions{}
	for _, opt := range opts {
		opt.apply(options)
	}
	if options.dialOptions == nil {
		options.dialOptions = []grpc.DialOption{
			grpc.WithInsecure(),
		}
	}
	return options
}

type connectOptions struct {
	dialOptions []grpc.DialOption
}

// ConnectOption is an option for connecting to a peer
type ConnectOption interface {
	apply(options *connectOptions)
}

// WithDialOption creates a dial option for the gRPC connection
func WithDialOption(option grpc.DialOption) ConnectOption {
	return &dialOptionOption{
		option: option,
	}
}

type dialOptionOption struct {
	option grpc.DialOption
}

func (o *dialOptionOption) apply(options *connectOptions) {
	options.dialOptions = append(options.dialOptions, o.option)
}

// WithDialOptions creates a dial option for the gRPC connection
func WithDialOptions(options ...grpc.DialOption) ConnectOption {
	return &dialOptionsOption{
		options: options,
	}
}

type dialOptionsOption struct {
	options []grpc.DialOption
}

func (o *dialOptionsOption) apply(options *connectOptions) {
	options.dialOptions = append(options.dialOptions, o.options...)
}
