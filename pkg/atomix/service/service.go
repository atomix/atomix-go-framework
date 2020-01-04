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
	streams "github.com/atomix/atomix-go-node/pkg/atomix/stream"
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
	Command(bytes []byte, stream streams.Stream)

	// Query applies a query to the state machine
	Query(bytes []byte, stream streams.Stream)
}

// service is an internal base for service implementations
type service struct {
	Scheduler Scheduler
	Executor  Executor
	Context   Context
}

// mutableContext is an internal context implementation which supports per-service indexes
type mutableContext struct {
	parent Context
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
	return c.parent.Index()
}

func (c *mutableContext) Timestamp() time.Time {
	return c.parent.Timestamp()
}

func (c *mutableContext) OperationType() OperationType {
	return c.op
}

func (c *mutableContext) setCommand() {
	c.op = OpTypeCommand
}

func (c *mutableContext) setQuery() {
	c.op = OpTypeQuery
}
