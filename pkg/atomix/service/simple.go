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
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"io"
)

// NewSimpleService returns a new simple primitive service
func NewSimpleService(parent Context) *SimpleService {
	scheduler := newScheduler()
	ctx := &mutableContext{}
	return &SimpleService{
		service: &service{
			Scheduler: newScheduler(),
			Executor:  newExecutor(),
			Context:   ctx,
		},
		scheduler: scheduler,
		context:   ctx,
		parent:    parent,
	}
}

// SimpleService is a Service implementation for primitive that do not support sessions
type SimpleService struct {
	Service
	*service
	scheduler *scheduler
	context   *mutableContext
	parent    Context
}

// Snapshot takes a snapshot of the service
func (s *SimpleService) Snapshot(writer io.Writer) error {
	bytes, err := s.Backup()
	if err != nil {
		return err
	}

	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(bytes)))

	_, err = writer.Write(length)
	if err != nil {
		return err
	}

	_, err = writer.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

// Install installs a snapshot to the service
func (s *SimpleService) Install(reader io.Reader) error {
	lengthBytes := make([]byte, 4)
	n, err := reader.Read(lengthBytes)
	if err != nil {
		return err
	}

	if n != 4 {
		return errors.New("malformed snapshot")
	}

	length := binary.BigEndian.Uint32(lengthBytes)
	bytes := make([]byte, length)
	_, err = reader.Read(bytes)
	if err != nil {
		return err
	}
	return s.Restore(bytes)
}

// Command handles a service command
func (s *SimpleService) Command(bytes []byte, ch chan<- Output) {
	s.context.setCommand(s.parent.Timestamp())
	command := &CommandRequest{}
	if err := proto.Unmarshal(bytes, command); err != nil {
		ch <- newFailure(err)
	} else {
		s.scheduler.runScheduledTasks(s.Context.Timestamp())

		// If the channel is non-nil, create a channel to pass to the service command and mutate the results.
		var commandCh chan Result
		if ch != nil {
			commandCh = make(chan Result)
			go func() {
				defer close(ch)
				for result := range commandCh {
					if result.Failed() {
						ch <- result.Output
					} else {
						ch <- newOutput(proto.Marshal(&CommandResponse{
							Context: &ResponseContext{
								Index: s.Context.Index(),
							},
							Output: result.Value,
						}))
					}
				}
			}()
		}

		if err := s.Executor.Execute(command.Name, command.Command, commandCh); err != nil {
			if ch != nil {
				ch <- newFailure(err)
				close(commandCh)
			}
			return
		}

		s.scheduler.runImmediateTasks()
		s.scheduler.runIndex(s.Context.Index())
	}
}

// Query handles a service query
func (s *SimpleService) Query(bytes []byte, ch chan<- Output) {
	query := &QueryRequest{}
	if err := proto.Unmarshal(bytes, query); err != nil {
		if ch != nil {
			ch <- newFailure(err)
		}
	} else {
		// If the channel is non-nil, create a channel to pass to the service query and mutate the results.
		var queryCh chan Result
		if ch != nil {
			queryCh = make(chan Result)
			go func() {
				defer close(ch)
				for result := range queryCh {
					if result.Failed() {
						ch <- result.Output
					} else {
						ch <- newOutput(proto.Marshal(&QueryResponse{
							Context: &ResponseContext{
								Index: s.Context.Index(),
							},
							Output: result.Value,
						}))
					}
				}
			}()
		}

		if query.Context.Index > s.Context.Index() {
			s.Scheduler.ScheduleIndex(query.Context.Index, func() {
				s.context.setQuery()
				if err := s.Executor.Execute(query.Name, query.Query, queryCh); err != nil {
					ch <- newFailure(err)
					close(queryCh)
				}
			})
		} else {
			s.context.setQuery()
			if err := s.Executor.Execute(query.Name, query.Query, queryCh); err != nil {
				ch <- newFailure(err)
				close(queryCh)
			}
		}
	}
}
