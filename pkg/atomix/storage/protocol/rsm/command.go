// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

// RequestID is a request identifier
type RequestID uint64

// ResponseID is a response identifier
type ResponseID uint64

// CommandID is a command identifier
type CommandID uint64

// Commands provides access to pending commands
type Commands interface {
	// Get gets a command by ID
	Get(CommandID) (Command, bool)
	// List lists all open commands
	List(OperationID) []Command
}

func newCommands() *primitiveCommands {
	return &primitiveCommands{
		commands: make(map[CommandID]Command),
	}
}

type primitiveCommands struct {
	commands map[CommandID]Command
}

func (s *primitiveCommands) add(command Command) {
	s.commands[command.ID()] = command
}

func (s *primitiveCommands) remove(command Command) {
	delete(s.commands, command.ID())
}

func (s *primitiveCommands) Get(commandID CommandID) (Command, bool) {
	command, ok := s.commands[commandID]
	return command, ok
}

func (s *primitiveCommands) List(operationID OperationID) []Command {
	commands := make([]Command, 0, len(s.commands))
	for _, command := range s.commands {
		if command.OperationID() == operationID {
			commands = append(commands, command)
		}
	}
	return commands
}

var _ Commands = (*primitiveCommands)(nil)

type CommandState int

const (
	CommandPending CommandState = iota
	CommandRunning
	CommandComplete
)

// Command is a command operation
type Command interface {
	Operation
	// ID returns the command identifier
	ID() CommandID
	// State returns the current command state
	State() CommandState
	// Watch watches the command state
	Watch(f func(CommandState)) Watcher
}
