package server

import (
	"context"
	"errors"
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

	// Create a write channel
	ch := make(chan *service.Result)

	// Write the request
	s.Client.Write(bytes, ch)

	// Wait for the result
	result, ok := <-ch
	if !ok {
		return nil, errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return nil, result.Error
	}

	// Decode and return the response
	serviceResponse := &service.ServiceResponse{}
	err = proto.Unmarshal(result.Output, serviceResponse)
	if err != nil {
		return nil, err
	}
	return serviceResponse.GetCommand(), nil
}

func (s *SessionizedServer) WriteStream(request []byte, header *headers.RequestHeader, ch chan<- *service.Result) error {
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

	streamCh := make(chan *service.Result)
	s.Client.Write(bytes, streamCh)

	// Create a goroutine to convert the results into raw form
	go func() {
		for result := range streamCh {
			if result.Failed() {
				ch <- result
			} else {
				serviceResponse := &service.ServiceResponse{}
				err := proto.Unmarshal(result.Output, serviceResponse)
				if err != nil {
					ch <- &service.Result{
						Index: result.Index,
						Error: err,
					}
				} else {
					ch <- &service.Result{
						Index:  result.Index,
						Output: serviceResponse.GetCommand(),
					}
				}
			}
		}
	}()

	return nil
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

	// Create a read channel
	ch := make(chan *service.Result)

	// Read the request
	s.Client.Read(bytes, ch)

	// Wait for the result
	result, ok := <-ch
	if !ok {
		return nil, errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return nil, result.Error
	}

	serviceResponse := &service.ServiceResponse{}
	err = proto.Unmarshal(result.Output, serviceResponse)
	if err != nil {
		return nil, err
	}
	return serviceResponse.GetQuery(), nil
}

func (s *SessionizedServer) ReadStream(request []byte, header *headers.RequestHeader, ch chan<- *service.Result) error {
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

	streamCh := make(chan *service.Result)
	s.Client.Read(bytes, streamCh)

	// Create a goroutine to convert the results into raw form
	go func() {
		for result := range streamCh {
			if result.Failed() {
				ch <- result
			} else {
				serviceResponse := &service.ServiceResponse{}
				err := proto.Unmarshal(result.Output, serviceResponse)
				if err != nil {
					ch <- &service.Result{
						Index: result.Index,
						Error: err,
					}
				} else {
					ch <- &service.Result{
						Index:  result.Index,
						Output: serviceResponse.GetQuery(),
					}
				}
			}
		}
	}()

	return nil
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
	responseHeader := &headers.ResponseHeader{
		SessionId:      header.SessionId,
		Index:          commandResponse.Context.Index,
		SequenceNumber: commandResponse.Context.Sequence,
	}
	return commandResponse.Output, responseHeader, nil
}

func (s *SessionizedServer) CommandStream(name string, input []byte, header *headers.RequestHeader, ch chan<- *SessionResult) error {
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

	resultCh := make(chan *service.Result)
	if err = s.WriteStream(bytes, header, resultCh); err != nil {
		return err
	}

	go func() {
		for result := range resultCh {
			if result.Failed() {
				ch <- &SessionResult{
					Result: &service.Result{
						Index: result.Index,
						Error: result.Error,
					},
				}
			} else {
				sessionResponse := &service.SessionResponse{}
				err = proto.Unmarshal(bytes, sessionResponse)
				if err != nil {
					ch <- &SessionResult{
						Result: &service.Result{
							Index: result.Index,
							Error: err,
						},
					}
				} else {
					commandResponse := sessionResponse.GetCommand()
					responseHeader := &headers.ResponseHeader{
						SessionId:      header.SessionId,
						Index:          commandResponse.Context.Index,
						SequenceNumber: commandResponse.Context.Sequence,
					}
					ch <- &SessionResult{
						Result: &service.Result{
							Index: result.Index,
							Error: result.Error,
						},
						Header: responseHeader,
					}
				}
			}
		}
	}()

	return nil
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
	responseHeader := &headers.ResponseHeader{
		SessionId:      header.SessionId,
		Index:          queryResponse.Context.Index,
		SequenceNumber: queryResponse.Context.Sequence,
	}
	return queryResponse.Output, responseHeader, nil
}

func (s *SessionizedServer) QueryStream(name string, input []byte, header *headers.RequestHeader, ch chan<- *SessionResult) error {
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

	resultCh := make(chan *service.Result)
	if err = s.ReadStream(bytes, header, resultCh); err != nil {
		return err
	}

	go func() {
		for result := range resultCh {
			if result.Failed() {
				ch <- &SessionResult{
					Result: &service.Result{
						Index: result.Index,
						Error: result.Error,
					},
				}
			} else {
				sessionResponse := &service.SessionResponse{}
				err = proto.Unmarshal(bytes, sessionResponse)
				if err != nil {
					ch <- &SessionResult{
						Result: &service.Result{
							Index: result.Index,
							Error: err,
						},
					}
				} else {
					queryResponse := sessionResponse.GetQuery()
					responseHeader := &headers.ResponseHeader{
						SessionId:      header.SessionId,
						Index:          queryResponse.Context.Index,
						SequenceNumber: queryResponse.Context.Sequence,
					}
					ch <- &SessionResult{
						Result: &service.Result{
							Index: result.Index,
							Error: result.Error,
						},
						Header: responseHeader,
					}
				}
			}
		}
	}()

	return nil
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

// SessionResult is a result for session-supporting servers containing session header information
type SessionResult struct {
	*service.Result
	Header *headers.ResponseHeader
}
