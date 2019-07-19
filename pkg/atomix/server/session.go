package server

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/atomix/atomix-go-node/proto/atomix/headers"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/duration"
	"time"
)

// SessionizedServer is a base server for servers that support sessions
type SessionizedServer struct {
	Client service.Client
	Type   string
}

func (s *SessionizedServer) Write(ctx context.Context, request []byte, header *headers.RequestHeader) ([]byte, error) {
	serviceRequest := &service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      s.Type,
			Name:      header.Name.Name,
			Namespace: header.Name.Namespace,
		},
		Request: &service.ServiceRequest_Command{
			Command: request,
		},
	}

	bytes, err := proto.Marshal(serviceRequest)
	if err != nil {
		return nil, err
	}

	bytes, err = s.Client.Write(bytes)
	if err != nil {
		return nil, err
	}

	serviceResponse := &service.ServiceResponse{}
	err = proto.Unmarshal(bytes, serviceResponse)
	if err != nil {
		return nil, err
	}
	return serviceResponse.GetCommand(), nil
}

func (s *SessionizedServer) WriteStream(request []byte, header *headers.RequestHeader, callback func([]byte, error)) error {
	serviceRequest := &service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      s.Type,
			Name:      header.Name.Name,
			Namespace: header.Name.Namespace,
		},
		Request: &service.ServiceRequest_Command{
			Command: request,
		},
	}

	bytes, err := proto.Marshal(serviceRequest)
	if err != nil {
		return err
	}

	return s.Client.WriteStream(bytes, &sessionStream{callback})
}

func (s *SessionizedServer) Read(ctx context.Context, request []byte, header *headers.RequestHeader) ([]byte, error) {
	serviceRequest := &service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      s.Type,
			Name:      header.Name.Name,
			Namespace: header.Name.Namespace,
		},
		Request: &service.ServiceRequest_Query{
			Query: request,
		},
	}

	bytes, err := proto.Marshal(serviceRequest)
	if err != nil {
		return nil, err
	}

	bytes, err = s.Client.Read(bytes)
	if err != nil {
		return nil, err
	}

	serviceResponse := &service.ServiceResponse{}
	err = proto.Unmarshal(bytes, serviceResponse)
	if err != nil {
		return nil, err
	}
	return serviceResponse.GetQuery(), nil
}

func (s *SessionizedServer) ReadStream(request []byte, header *headers.RequestHeader, callback func([]byte, error)) error {
	serviceRequest := &service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      s.Type,
			Name:      header.Name.Name,
			Namespace: header.Name.Namespace,
		},
		Request: &service.ServiceRequest_Query{
			Query: request,
		},
	}

	bytes, err := proto.Marshal(serviceRequest)
	if err != nil {
		return err
	}

	return s.Client.ReadStream(bytes, &sessionStream{callback})
}

func (s *SessionizedServer) Command(ctx context.Context, name string, input []byte, header *headers.RequestHeader) ([]byte, *headers.ResponseHeader, error) {
	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_Command{
			Command: &service.SessionCommandRequest{
				Context: &service.SessionCommandContext{
					SessionId:      header.SessionId,
					SequenceNumber: header.SequenceNumber,
				},
				Name:  name,
				Input: input,
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return nil, nil, err
	}

	bytes, err = s.Write(ctx, bytes, header)
	if err != nil {
		return nil, nil, err
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(bytes, sessionResponse)
	if err != nil {
		return nil, nil, err
	}

	commandResponse := sessionResponse.GetCommand()
	streams := make([]*headers.StreamHeader, len(commandResponse.Context.Streams))
	for i, stream := range commandResponse.Context.Streams {
		streams[i] = &headers.StreamHeader{
			StreamId:       stream.StreamId,
			Index:          stream.Index,
			LastItemNumber: stream.Sequence,
		}
	}

	responseHeader := &headers.ResponseHeader{
		SessionId:      header.SessionId,
		Index:          commandResponse.Context.Index,
		SequenceNumber: commandResponse.Context.Sequence,
		Streams:        streams,
	}
	return commandResponse.Output, responseHeader, nil
}

func (s *SessionizedServer) CommandStream(name string, input []byte, header *headers.RequestHeader, handler func([]byte, *headers.ResponseHeader, error)) error {
	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_Command{
			Command: &service.SessionCommandRequest{
				Context: &service.SessionCommandContext{
					SessionId:      header.SessionId,
					SequenceNumber: header.SequenceNumber,
				},
				Name:  name,
				Input: input,
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return err
	}

	return s.WriteStream(bytes, header, func(bytes []byte, err error) {
		if err != nil {
			handler(nil, nil, err)
		} else if bytes != nil {
			sessionResponse := &service.SessionResponse{}
			err = proto.Unmarshal(bytes, sessionResponse)
			if err != nil {
				handler(nil, nil, err)
			} else {
				commandResponse := sessionResponse.GetCommand()
				streams := make([]*headers.StreamHeader, len(commandResponse.Context.Streams))
				for i, stream := range commandResponse.Context.Streams {
					streams[i] = &headers.StreamHeader{
						StreamId:       stream.StreamId,
						Index:          stream.Index,
						LastItemNumber: stream.Sequence,
					}
				}

				responseHeader := &headers.ResponseHeader{
					SessionId:      header.SessionId,
					Index:          commandResponse.Context.Index,
					SequenceNumber: commandResponse.Context.Sequence,
					Streams:        streams,
				}
				handler(commandResponse.Output, responseHeader, nil)
			}
		}
	})
}

func (s *SessionizedServer) Query(ctx context.Context, name string, input []byte, header *headers.RequestHeader) ([]byte, *headers.ResponseHeader, error) {
	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_Query{
			Query: &service.SessionQueryRequest{
				Context: &service.SessionQueryContext{
					SessionId:          header.SessionId,
					LastIndex:          header.Index,
					LastSequenceNumber: header.SequenceNumber,
				},
				Name:  name,
				Input: input,
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return nil, nil, err
	}

	bytes, err = s.Write(ctx, bytes, header)
	if err != nil {
		return nil, nil, err
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(bytes, sessionResponse)
	if err != nil {
		return nil, nil, err
	}

	queryResponse := sessionResponse.GetQuery()
	streams := make([]*headers.StreamHeader, len(queryResponse.Context.Streams))
	for i, stream := range queryResponse.Context.Streams {
		streams[i] = &headers.StreamHeader{
			StreamId:       stream.StreamId,
			Index:          stream.Index,
			LastItemNumber: stream.Sequence,
		}
	}

	responseHeader := &headers.ResponseHeader{
		SessionId:      header.SessionId,
		Index:          queryResponse.Context.Index,
		SequenceNumber: queryResponse.Context.Sequence,
		Streams:        streams,
	}
	return queryResponse.Output, responseHeader, nil
}

func (s *SessionizedServer) QueryStream(name string, input []byte, header *headers.RequestHeader, handler func([]byte, *headers.ResponseHeader, error)) error {
	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_Query{
			Query: &service.SessionQueryRequest{
				Context: &service.SessionQueryContext{
					SessionId:          header.SessionId,
					LastIndex:          header.Index,
					LastSequenceNumber: header.SequenceNumber,
				},
				Name:  name,
				Input: input,
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return err
	}

	return s.ReadStream(bytes, header, func(bytes []byte, err error) {
		if err != nil {
			handler(nil, nil, err)
		} else if bytes != nil {
			sessionResponse := &service.SessionResponse{}
			err = proto.Unmarshal(bytes, sessionResponse)
			if err != nil {
				handler(nil, nil, err)
			} else {
				queryResponse := sessionResponse.GetQuery()
				streams := make([]*headers.StreamHeader, len(queryResponse.Context.Streams))
				for i, stream := range queryResponse.Context.Streams {
					streams[i] = &headers.StreamHeader{
						StreamId:       stream.StreamId,
						Index:          stream.Index,
						LastItemNumber: stream.Sequence,
					}
				}

				responseHeader := &headers.ResponseHeader{
					SessionId:      header.SessionId,
					Index:          queryResponse.Context.Index,
					SequenceNumber: queryResponse.Context.Sequence,
					Streams:        streams,
				}
				handler(queryResponse.Output, responseHeader, nil)
			}
		}
	})
}

func (s *SessionizedServer) OpenSession(ctx context.Context, header *headers.RequestHeader, timeout *duration.Duration) (uint64, error) {
	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_OpenSession{
			OpenSession: &service.OpenSessionRequest{
				Timeout: (timeout.Seconds + int64(timeout.Nanos)) / int64(time.Millisecond),
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return 0, err
	}

	bytes, err = s.Write(ctx, bytes, header)
	if err != nil {
		return 0, err
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(bytes, sessionResponse)
	if err != nil {
		return 0, err
	}

	return sessionResponse.GetOpenSession().SessionId, nil
}

func (s *SessionizedServer) KeepAliveSession(ctx context.Context, header *headers.RequestHeader) error {
	streams := make(map[uint64]uint64)
	for _, stream := range header.Streams {
		streams[stream.StreamId] = stream.LastItemNumber
	}

	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_KeepAlive{
			KeepAlive: &service.KeepAliveRequest{
				SessionId:       header.SessionId,
				CommandSequence: header.SequenceNumber,
				Streams:         streams,
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return err
	}

	bytes, err = s.Write(ctx, bytes, header)
	if err != nil {
		return err
	}

	sessionResponse := &service.SessionResponse{}
	return proto.Unmarshal(bytes, sessionResponse)
}

func (s *SessionizedServer) CloseSession(ctx context.Context, header *headers.RequestHeader) error {
	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_CloseSession{
			CloseSession: &service.CloseSessionRequest{
				SessionId: header.SessionId,
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return err
	}

	bytes, err = s.Write(ctx, bytes, header)
	if err != nil {
		return err
	}

	sessionResponse := &service.SessionResponse{}
	return proto.Unmarshal(bytes, sessionResponse)
}

type sessionStream struct {
	callback func([]byte, error)
}

func (s *sessionStream) Next(value []byte) {
	serviceResponse := &service.ServiceResponse{}
	err := proto.Unmarshal(value, serviceResponse)
	if err != nil {
		s.callback(nil, err)
	} else {
		s.callback(serviceResponse.GetCommand(), nil)
	}
}

func (s *sessionStream) Complete() {
	s.callback(nil, nil)
}

func (s *sessionStream) Fail(err error) {
	s.callback(nil, err)
}
