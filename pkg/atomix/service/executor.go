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
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/stream"
)

// Executor executes primitive operations
type Executor interface {
	// RegisterUnary registers a unary primitive operation
	RegisterUnary(op string, callback func([]byte) ([]byte, error))

	// RegisterStream registers a new primitive operation
	RegisterStream(op string, callback func([]byte, stream.Stream))

	// Execute executes a primitive operation
	Execute(name string, op []byte, stream stream.Stream) error
}

// newExecutor returns a new executor
func newExecutor() Executor {
	return &executor{
		operations: make(map[string]func([]byte, stream.Stream)),
	}
}

// executor is an implementation of the Executor interface
type executor struct {
	Executor
	operations map[string]func([]byte, stream.Stream)
}

func (e *executor) RegisterUnary(op string, callback func([]byte) ([]byte, error)) {
	e.operations[op] = func(value []byte, stream stream.Stream) {
		stream.Result(callback(value))
		stream.Close()
	}
}

func (e *executor) RegisterStream(op string, callback func([]byte, stream.Stream)) {
	e.operations[op] = callback
}

func (e *executor) Execute(name string, bytes []byte, stream stream.Stream) error {
	op, ok := e.operations[name]
	if !ok {
		return fmt.Errorf("unknown operation %s", name)
	}
	op(bytes, stream)
	return nil
}
