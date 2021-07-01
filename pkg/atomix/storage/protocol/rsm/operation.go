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

package rsm

// OperationID is an operation identifier
type OperationID string

// OperationRegistry executes primitive operations
type OperationRegistry interface {
	// RegisterUnary registers a unary primitive operation
	RegisterUnary(id OperationID, callback func([]byte, Session) ([]byte, error))

	// RegisterStream registers a new primitive operation
	RegisterStream(id OperationID, opener func(Stream), callback func([]byte, Stream) error, closer func(Stream))

	// GetOperation returns an operation by name
	GetOperation(id OperationID) Operation
}

// Operation is the base interface for primitive operations
type Operation interface{}

// UnaryOperation is a primitive operation that returns a result
type UnaryOperation interface {
	// Execute executes the operation
	Execute(bytes []byte, session Session) ([]byte, error)
}

// StreamingOperation is a primitive operation that returns a stream
type StreamingOperation interface {
	Open(stream Stream)
	Close(stream Stream)
	// Execute executes the operation
	Execute(bytes []byte, stream Stream) error
}

// newOperationRegistry returns a new executor
func newOperationRegistry() OperationRegistry {
	return &executor{
		operations: make(map[OperationID]Operation),
	}
}

// executor is an implementation of the OperationRegistry interface
type executor struct {
	OperationRegistry
	operations map[OperationID]Operation
}

func (e *executor) RegisterUnary(id OperationID, callback func([]byte, Session) ([]byte, error)) {
	e.operations[id] = &unaryOperation{
		f: callback,
	}
}

func (e *executor) RegisterStream(id OperationID, opener func(Stream), callback func([]byte, Stream) error, closer func(Stream)) {
	e.operations[id] = &streamingOperation{
		opener: opener,
		f:      callback,
		closer: closer,
	}
}

func (e *executor) GetOperation(id OperationID) Operation {
	return e.operations[id]
}

// unaryOperation is an implementation of the UnaryOperation interface
type unaryOperation struct {
	f func([]byte, Session) ([]byte, error)
}

func (o *unaryOperation) Execute(bytes []byte, session Session) ([]byte, error) {
	return o.f(bytes, session)
}

// streamingOperation is an implementation of the StreamingOperation interface
type streamingOperation struct {
	opener func(Stream)
	f      func([]byte, Stream) error
	closer func(Stream)
}

func (o *streamingOperation) Open(stream Stream) {
	o.opener(stream)
}

func (o *streamingOperation) Execute(bytes []byte, stream Stream) error {
	return o.f(bytes, stream)
}

func (o *streamingOperation) Close(stream Stream) {
	o.closer(stream)
}
