package service

import (
	"encoding/binary"
	"errors"
	"github.com/atomix/atomix-go-node/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
	"io"
)

// SimpleService is a Service implementation for primitive that do not support sessions
type SimpleService struct {
	Service
	scheduler    Scheduler
	executor     Executor
	ctx          Context
	indexQueries map[uint64][]*QueryRequest
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

func (s *SimpleService) Command(bytes []byte, callback func([]byte, error)) {
	command := &CommandRequest{}
	if err := proto.Unmarshal(bytes, command); err != nil {
		callback(nil, err)
	} else {
		scheduler := s.scheduler.(*scheduler)
		scheduler.runScheduledTasks(s.ctx.Timestamp())
		s.executor.Execute(command.Name, command.Command, func(bytes []byte, err error) {
			if err != nil {
				callback(nil, err)
			} else {
				callback(proto.Marshal(&CommandResponse{
					Context: &ResponseContext{
						Index: s.ctx.Index(),
					},
					Output: bytes,
				}))
			}
		})
		scheduler.runImmediateTasks()
		scheduler.runIndex(s.ctx.Index())
	}
}

func (s *SimpleService) CommandStream(bytes []byte, stream stream.Stream, callback func(error)) {
	command := &CommandRequest{}
	if err := proto.Unmarshal(bytes, command); err != nil {
		callback(err)
	} else {
		scheduler := s.scheduler.(*scheduler)
		scheduler.runScheduledTasks(s.ctx.Timestamp())
		s.executor.ExecuteStream(command.Name, command.Command, stream, callback)
		scheduler.runImmediateTasks()
		scheduler.runIndex(s.ctx.Index())
	}
}

func (s *SimpleService) Query(bytes []byte, callback func([]byte, error)) {
	query := &QueryRequest{}
	if err := proto.Unmarshal(bytes, query); err != nil {
		callback(nil, err)
	} else {
		f := func(bytes []byte, err error) {
			if err != nil {
				callback(nil, err)
			} else {
				callback(proto.Marshal(&QueryResponse{
					Context: &ResponseContext{
						Index: s.ctx.Index(),
					},
					Output: bytes,
				}))
			}
		}
		if query.Context.Index > s.ctx.Index() {
			s.scheduler.ScheduleIndex(query.Context.Index, func() {
				s.executor.Execute(query.Name, query.Query, f)
			})
		} else {
			s.executor.Execute(query.Name, query.Query, f)
		}
	}
}

func (s *SimpleService) QueryStream(bytes []byte, stream stream.Stream, callback func(error)) {
	query := &QueryRequest{}
	if err := proto.Unmarshal(bytes, query); err != nil {
		callback(err)
	} else {
		if query.Context.Index > s.ctx.Index() {
			s.scheduler.ScheduleIndex(query.Context.Index, func() {
				s.executor.ExecuteStream(query.Name, query.Query, stream, callback)
			})
		} else {
			s.executor.ExecuteStream(query.Name, query.Query, stream, callback)
		}
	}
}
