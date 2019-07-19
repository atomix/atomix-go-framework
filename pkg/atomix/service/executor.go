package service

import (
	"errors"
	"fmt"
)

// Executor executes primitive operations
type Executor interface {
	// Register registers a new primitive operation
	Register(op string, callback func([]byte) ([]byte, error))

	// RegisterAsync registers an asynchronous primitive operation
	RegisterAsync(op string, callback func([]byte, func([]byte, error)))

	// Execute executes a primitive operation
	Execute(name string, op []byte, callback func([]byte, error))

	// RegisterStream registers a streaming primitive operation
	RegisterStream(op string, callback func([]byte, Stream) error)

	// RegisterStreamAsync registers an asynchronous streaming primitive operation
	RegisterStreamAsync(op string, callback func([]byte, Stream, func(error)))

	// ExecuteStream executes a streaming primitive operation
	ExecuteStream(name string, op []byte, stream Stream, callback func(error))
}

// newExecutor returns a new executor
func newExecutor() Executor {
	return &executor{
		operations:          make(map[string]func([]byte, func([]byte, error))),
		streamingOperations: make(map[string]func([]byte, Stream, func(error))),
	}
}

// executor is an implementation of the Executor interface
type executor struct {
	Executor
	operations          map[string]func([]byte, func([]byte, error))
	streamingOperations map[string]func([]byte, Stream, func(error))
}

func (e *executor) Register(op string, callback func([]byte) ([]byte, error)) {
	e.operations[op] = func(bytes []byte, f func([]byte, error)) {
		f(callback(bytes))
	}
}

func (e *executor) RegisterAsync(op string, callback func([]byte, func([]byte, error))) {
	e.operations[op] = callback
}

func (e *executor) Execute(name string, bytes []byte, callback func([]byte, error)) {
	op, ok := e.operations[name]
	if !ok {
		callback(nil, errors.New(fmt.Sprintf("unknown operation %s", name)))
	} else {
		op(bytes, callback)
	}
}

func (e *executor) RegisterStream(op string, callback func([]byte, Stream) error) {
	e.streamingOperations[op] = func(bytes []byte, stream Stream, f func(error)) {
		f(callback(bytes, stream))
	}
}

func (e *executor) RegisterStreamAsync(op string, callback func([]byte, Stream, func(error))) {
	e.streamingOperations[op] = callback
}

func (e *executor) ExecuteStream(name string, bytes []byte, stream Stream, callback func(error)) {
	op, ok := e.streamingOperations[name]
	if !ok {
		callback(errors.New(fmt.Sprintf("unknown operation %s", name)))
	} else {
		op(bytes, stream, callback)
	}
}
