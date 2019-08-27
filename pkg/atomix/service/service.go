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

package service

import "time"

// OperationType is the type for a service operation
type OperationType string

const (
	// OpTypeCommand is an OperationType indicating a command operation
	OpTypeCommand OperationType = "command"
	// OpTypeQuery is an OperationType indicating a query operation
	OpTypeQuery OperationType = "query"
)

// Service is an interface for primitive services
type Service interface {
	StateMachine

	// Backup must be implemented by services to return the serialized state of the service
	Backup() ([]byte, error)

	// Restore must be implemented by services to restore the state of the service from a serialized backup
	Restore(bytes []byte) error
}

// Registry is a registry of service types
type Registry struct {
	types map[string]func(ctx Context) Service
}

// Register registers a new primitive type
func (r *Registry) Register(name string, f func(ctx Context) Service) {
	r.types[name] = f
}

// getType returns a service type by name
func (r *Registry) getType(name string) func(sctx Context) Service {
	return r.types[name]
}

// NewServiceRegistry returns a new primitive type registry
func NewServiceRegistry() *Registry {
	return &Registry{types: make(map[string]func(ctx Context) Service)}
}

// service is an internal base for service implementations
type service struct {
	Scheduler Scheduler
	Executor  Executor
	Context   Context
}

// NewResult returns a new result with the given output and error
func (s *service) NewResult(value []byte, err error) Result {
	return Result{
		Index: s.Context.Index(),
		Output: Output{
			Value: value,
			Error: err,
		},
	}
}

// NewSuccess returns a new successful result with the given output
func (s *service) NewSuccess(value []byte) Result {
	return Result{
		Index: s.Context.Index(),
		Output: Output{
			Value: value,
		},
	}
}

// NewFailure returns a new failure result with the given error
func (s *service) NewFailure(err error) Result {
	return Result{
		Index: s.Context.Index(),
		Output: Output{
			Error: err,
		},
	}
}

// mutableContext is an internal context implementation which supports per-service indexes
type mutableContext struct {
	Context
	index uint64
	time  time.Time
	op    OperationType
}

func (c *mutableContext) Index() uint64 {
	return c.index
}

func (c *mutableContext) Timestamp() time.Time {
	return c.time
}

func (c *mutableContext) OperationType() OperationType {
	return c.op
}

func (c *mutableContext) setCommand(time time.Time) {
	c.index = c.index + 1
	c.time = time
	c.op = OpTypeCommand
}

func (c *mutableContext) setQuery() {
	c.op = OpTypeQuery
}
