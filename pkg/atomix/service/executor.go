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
	"errors"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/stream"
)

// Executor executes primitive operations
type Executor interface {
	// RegisterBackup registers a backup function
	RegisterBackup(callback func() ([]byte, error))

	// RegisterRestore registers a restore function
	RegisterRestore(callback func([]byte) error)

	// RegisterUnaryOp registers a unary primitive operation
	RegisterUnaryOp(op string, callback func([]byte) ([]byte, error))

	// RegisterStreamOp registers a new primitive operation
	RegisterStreamOp(op string, callback func([]byte, stream.Stream))

	// Backup backs up a service
	Backup() ([]byte, error)

	// Restore restores a service
	Restore([]byte) error

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
	backup     func() ([]byte, error)
	restore    func([]byte) error
	operations map[string]func([]byte, stream.Stream)
}

func (e *executor) RegisterBackup(callback func() ([]byte, error)) {
	e.backup = callback
}

func (e *executor) RegisterRestore(callback func([]byte) error) {
	e.restore = callback
}

func (e *executor) RegisterUnaryOp(op string, callback func([]byte) ([]byte, error)) {
	e.operations[op] = func(value []byte, stream stream.Stream) {
		stream.Result(callback(value))
		stream.Close()
	}
}

func (e *executor) RegisterStreamOp(op string, callback func([]byte, stream.Stream)) {
	e.operations[op] = callback
}

func (e *executor) Backup() ([]byte, error) {
	backup := e.backup
	if backup == nil {
		return nil, errors.New("invalid service: no backup function registered")
	}
	return backup()
}

func (e *executor) Restore(bytes []byte) error {
	restore := e.restore
	if restore == nil {
		return errors.New("invalid service: no restore function registered")
	}
	return restore(bytes)
}

func (e *executor) Execute(name string, bytes []byte, stream stream.Stream) error {
	op, ok := e.operations[name]
	if !ok {
		return fmt.Errorf("unknown operation %s", name)
	}
	op(bytes, stream)
	return nil
}
