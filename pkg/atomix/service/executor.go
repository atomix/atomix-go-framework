package service

import (
	"errors"
	"fmt"
)

// Executor executes primitive operations
type Executor interface {
	// Register registers a new primitive operation
	Register(op string, callback func([]byte, chan<- *Result))

	// Execute executes a primitive operation
	Execute(name string, op []byte, ch chan<- *Result) error
}

// newExecutor returns a new executor
func newExecutor() Executor {
	return &executor{
		operations: make(map[string]func([]byte, chan<- *Result)),
	}
}

// executor is an implementation of the Executor interface
type executor struct {
	Executor
	operations map[string]func([]byte, chan<- *Result)
}

func (e *executor) Register(op string, callback func([]byte, chan<- *Result)) {
	e.operations[op] = callback
}

func (e *executor) Execute(name string, bytes []byte, ch chan<- *Result) error {
	op, ok := e.operations[name]
	if !ok {
		return errors.New(fmt.Sprintf("unknown operation %s", name))
	} else {
		op(bytes, ch)
	}
	return nil
}
