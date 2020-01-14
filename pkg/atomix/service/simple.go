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
	streams "github.com/atomix/atomix-go-node/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
	"io"
)

// NewSimpleService returns a new simple primitive service
func NewSimpleService(parent Context) *SimpleService {
	scheduler := newScheduler()
	ctx := &mutableContext{
		parent: parent,
	}
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
	return nil
}

// Install installs a snapshot to the service
func (s *SimpleService) Install(reader io.Reader) error {
	return nil
}

// Command handles a service command
func (s *SimpleService) Command(bytes []byte, stream streams.WriteStream) {
	s.context.setCommand()
	command := &CommandRequest{}
	if err := proto.Unmarshal(bytes, command); err != nil {
		stream.Error(err)
		stream.Close()
	} else {
		s.scheduler.runScheduledTasks(s.Context.Timestamp())
		responseStream := streams.NewEncodingStream(stream, func(value interface{}) (interface{}, error) {
			return proto.Marshal(&CommandResponse{
				Context: &ResponseContext{
					Index: s.Context.Index(),
				},
				Output: value.([]byte),
			})
		})

		operation := s.Executor.GetOperation(command.Name)
		if operation == nil {
			responseStream.Error(fmt.Errorf("unknown operation: %s", command.Name))
			responseStream.Close()
			return
		}

		if streamOp, ok := operation.(StreamingOperation); ok {
			stream.Result(proto.Marshal(&CommandResponse{
				Context: &ResponseContext{
					Index: s.Context.Index(),
					Type:  ResponseType_OPEN_STREAM,
				},
			}))

			responseStream = streams.NewCloserStream(responseStream, func(_ streams.WriteStream) {
				stream.Result(proto.Marshal(&CommandResponse{
					Context: &ResponseContext{
						Index: s.Context.Index(),
						Type:  ResponseType_CLOSE_STREAM,
					},
				}))
			})

			streamOp.Execute(command.Command, responseStream)
		} else if unaryOp, ok := operation.(UnaryOperation); ok {
			responseStream.Result(unaryOp.Execute(command.Command))
			responseStream.Close()
		}

		s.scheduler.runImmediateTasks()
		s.scheduler.runIndex(s.Context.Index())
	}
}

// Query handles a service query
func (s *SimpleService) Query(bytes []byte, stream streams.WriteStream) {
	query := &QueryRequest{}
	if err := proto.Unmarshal(bytes, query); err != nil {
		stream.Error(err)
		stream.Close()
	} else {
		if query.Context.Index > s.Context.Index() {
			s.Scheduler.ScheduleIndex(query.Context.Index, func() {
				s.execute(query, stream)
			})
		} else {
			s.execute(query, stream)
		}
	}
}

func (s *SimpleService) execute(query *QueryRequest, stream streams.WriteStream) {
	s.context.setQuery()

	responseStream := streams.NewEncodingStream(stream, func(value interface{}) (interface{}, error) {
		return proto.Marshal(&QueryResponse{
			Context: &ResponseContext{
				Index: s.Context.Index(),
			},
			Output: value.([]byte),
		})
	})

	operation := s.Executor.GetOperation(query.Name)
	if operation == nil {
		responseStream.Error(fmt.Errorf("unknown operation: %s", query.Name))
		responseStream.Close()
		return
	}

	if streamOp, ok := operation.(StreamingOperation); ok {
		stream.Result(proto.Marshal(&QueryResponse{
			Context: &ResponseContext{
				Index: s.Context.Index(),
				Type:  ResponseType_OPEN_STREAM,
			},
		}))

		responseStream = streams.NewCloserStream(responseStream, func(_ streams.WriteStream) {
			stream.Result(proto.Marshal(&QueryResponse{
				Context: &ResponseContext{
					Index: s.Context.Index(),
					Type:  ResponseType_CLOSE_STREAM,
				},
			}))
		})

		streamOp.Execute(query.Query, responseStream)
	} else if unaryOp, ok := operation.(UnaryOperation); ok {
		responseStream.Result(unaryOp.Execute(query.Query))
		responseStream.Close()
	}
}
