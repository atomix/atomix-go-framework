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

package primitive

import (
	"github.com/atomix/go-framework/pkg/atomix/stream"
)

// Executor executes primitive operations
type Executor interface {
	// RegisterUnaryOperation registers a unary primitive operation
	RegisterUnaryOperation(name string, callback func([]byte) ([]byte, error))

	// RegisterStreamOperation registers a new primitive operation
	RegisterStreamOperation(name string, callback func([]byte, stream.WriteStream))

	// GetOperation returns an operation by name
	GetOperation(name string) Operation
}

// Operation is the base interface for primitive operations
type Operation interface{}

// UnaryOperation is a primitive operation that returns a result
type UnaryOperation interface {
	// Execute executes the operation
	Execute(bytes []byte) ([]byte, error)
}

// StreamingOperation is a primitive operation that returns a stream
type StreamingOperation interface {
	// Execute executes the operation
	Execute(bytes []byte, stream stream.WriteStream)
}

// newExecutor returns a new executor
func newExecutor() Executor {
	return &executor{
		operations: make(map[string]Operation),
	}
}

// executor is an implementation of the Executor interface
type executor struct {
	Executor
	operations map[string]Operation
}

func (e *executor) RegisterUnaryOperation(name string, callback func([]byte) ([]byte, error)) {
	e.operations[name] = &unaryOperation{
		f: callback,
	}
}

func (e *executor) RegisterStreamOperation(name string, callback func([]byte, stream.WriteStream)) {
	e.operations[name] = &streamingOperation{
		f: callback,
	}
}

func (e *executor) GetOperation(name string) Operation {
	return e.operations[name]
}

// unaryOperation is an implementation of the UnaryOperation interface
type unaryOperation struct {
	f func([]byte) ([]byte, error)
}

func (o *unaryOperation) Execute(bytes []byte) ([]byte, error) {
	return o.f(bytes)
}

// streamingOperation is an implementation of the StreamingOperation interface
type streamingOperation struct {
	f func([]byte, stream.WriteStream)
}

func (o *streamingOperation) Execute(bytes []byte, stream stream.WriteStream) {
	o.f(bytes, stream)
}
