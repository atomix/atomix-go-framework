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
)

// Executor executes primitive operations
type Executor interface {
	// Register registers a new primitive operation
	Register(op string, callback func([]byte, chan<- Result))

	// Execute executes a primitive operation
	Execute(name string, op []byte, ch chan<- Result) error
}

// newExecutor returns a new executor
func newExecutor() Executor {
	return &executor{
		operations: make(map[string]func([]byte, chan<- Result)),
	}
}

// executor is an implementation of the Executor interface
type executor struct {
	Executor
	operations map[string]func([]byte, chan<- Result)
}

func (e *executor) Register(op string, callback func([]byte, chan<- Result)) {
	e.operations[op] = callback
}

func (e *executor) Execute(name string, bytes []byte, ch chan<- Result) error {
	op, ok := e.operations[name]
	if !ok {
		return fmt.Errorf("unknown operation %s", name)
	}
	op(bytes, ch)
	return nil
}
