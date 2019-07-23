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
	ctx := &context{}
	return &SimpleService{
		Scheduler: newScheduler(),
		scheduler: scheduler,
		Executor:  newExecutor(),
		Context:   ctx,
		context:   ctx,
		parent:    parent,
	}
}

// SimpleService is a Service implementation for primitive that do not support sessions
type SimpleService struct {
	Service
	*service
	Scheduler Scheduler
	scheduler *scheduler
	Executor  Executor
	Context   Context
	context   *context
	parent    Context
}

func (s *SimpleService) Snapshot(writer io.Writer) error {
	bytes, err := s.Backup()
	if err != nil {
		return err
	} else {
		length := make([]byte, 4)
		binary.BigEndian.PutUint32(length, uint32(len(bytes)))
		writer.Write(length)
		writer.Write(bytes)
	}
	return nil
}

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
	n, err = reader.Read(bytes)
	if err != nil {
		return err
	}
	return s.Restore(bytes)
}

func (s *SimpleService) Command(bytes []byte, ch chan<- *Result) {
	s.context.setCommand(s.parent.Timestamp())
	command := &CommandRequest{}
	if err := proto.Unmarshal(bytes, command); err != nil {
		ch <- s.NewFailure(err)
	} else {
		s.scheduler.runScheduledTasks(s.Context.Timestamp())

		// If the channel is non-nil, create a channel to pass to the service command and mutate the results.
		var commandCh chan *Result
		if ch != nil {
			commandCh = make(chan *Result)
			go func() {
				for result := range commandCh {
					if result.Failed() {
						ch <- result
					} else {
						ch <- result.mutateResult(proto.Marshal(&CommandResponse{
							Context: &ResponseContext{
								Index: s.Context.Index(),
							},
							Output: result.Output,
						}))
					}
				}
				close(ch)
			}()
		}

		if err := s.Executor.Execute(command.Name, command.Command, commandCh); err != nil {
			if ch != nil {
				ch <- s.NewFailure(err)
				close(commandCh)
			}
			return
		}

		s.scheduler.runImmediateTasks()
		s.scheduler.runIndex(s.Context.Index())
	}
}

func (s *SimpleService) Query(bytes []byte, ch chan<- *Result) {
	query := &QueryRequest{}
	if err := proto.Unmarshal(bytes, query); err != nil {
		if ch != nil {
			ch <- s.NewFailure(err)
		}
	} else {
		// If the channel is non-nil, create a channel to pass to the service query and mutate the results.
		var queryCh chan *Result
		if ch != nil {
			queryCh = make(chan *Result)
			go func() {
				for result := range queryCh {
					if result.Failed() {
						ch <- result
					} else {
						ch <- result.mutateResult(proto.Marshal(&QueryResponse{
							Context: &ResponseContext{
								Index: s.Context.Index(),
							},
							Output: result.Output,
						}))
					}
				}
				close(ch)
			}()
		}

		if query.Context.Index > s.Context.Index() {
			s.Scheduler.ScheduleIndex(query.Context.Index, func() {
				s.context.setQuery()
				if err := s.Executor.Execute(query.Name, query.Query, queryCh); err != nil {
					ch <- s.NewFailure(err)
					close(queryCh)
				}
			})
		} else {
			s.context.setQuery()
			if err := s.Executor.Execute(query.Name, query.Query, queryCh); err != nil {
				ch <- s.NewFailure(err)
				close(queryCh)
			}
		}
	}
}
