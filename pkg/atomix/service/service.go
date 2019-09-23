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

import (
	"io"
	"time"
)

// OperationType is the type for a service operation
type OperationType string

const (
	// OpTypeCommand is an OperationType indicating a command operation
	OpTypeCommand OperationType = "command"
	// OpTypeQuery is an OperationType indicating a query operation
	OpTypeQuery OperationType = "query"
)

// Context provides information about the context within which a state machine is running
type Context interface {
	// Node is the local node identifier
	Node() string

	// Name is the service name
	Name() string

	// Namespace is the service namespace name
	Namespace() string

	// Index returns the current index of the state machine
	Index() uint64

	// Timestamp returns a deterministic, monotonically increasing timestamp
	Timestamp() time.Time

	// OperationType returns the type of the operation currently being executed against the state machine
	OperationType() OperationType
}

// Service is an interface for primitive services
type Service interface {
	// Snapshot writes the state machine snapshot to the given writer
	Snapshot(writer io.Writer) error

	// Install reads the state machine snapshot from the given reader
	Install(reader io.Reader) error

	// CanDelete returns a bool indicating whether the node can delete changes up to the given index without affecting
	// the correctness of the state machine
	CanDelete(index uint64) bool

	// Command applies a command to the state machine
	Command(bytes []byte, ch chan<- Result)

	// Query applies a query to the state machine
	Query(bytes []byte, ch chan<- Result)

	// Backup must be implemented by services to return the serialized state of the service
	Backup() ([]byte, error)

	// Restore must be implemented by services to restore the state of the service from a serialized backup
	Restore(bytes []byte) error
}

func newResult(index uint64, value []byte, err error) Result {
	return Result{
		Index: index,
		Value: value,
		Error: err,
	}
}

func newFailure(index uint64, err error) Result {
	return Result{
		Index: index,
		Error: err,
	}
}

func fail(ch chan<- Result, index uint64, err error) {
	ch <- newFailure(index, err)
	close(ch)
}

// Result is a state machine operation result
type Result struct {
	Index uint64
	Value []byte
	Error error
}

// Failed returns a boolean indicating whether the operation failed
func (r Result) Failed() bool {
	return r.Error != nil
}

// Succeeded returns a boolean indicating whether the operation was successful
func (r Result) Succeeded() bool {
	return !r.Failed()
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
		Value: value,
		Error: err,
	}
}

// NewSuccess returns a new successful result with the given output
func (s *service) NewSuccess(value []byte) Result {
	return Result{
		Index: s.Context.Index(),
		Value: value,
	}
}

// NewFailure returns a new failure result with the given error
func (s *service) NewFailure(err error) Result {
	return Result{
		Index: s.Context.Index(),
		Error: err,
	}
}

// mutableContext is an internal context implementation which supports per-service indexes
type mutableContext struct {
	parent Context
	index  uint64
	time   time.Time
	op     OperationType
}

func (c *mutableContext) Node() string {
	return c.parent.Node()
}

func (c *mutableContext) Name() string {
	return c.parent.Name()
}

func (c *mutableContext) Namespace() string {
	return c.parent.Namespace()
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
