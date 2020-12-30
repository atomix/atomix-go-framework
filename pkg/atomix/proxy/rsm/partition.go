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

package rsm

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/storage/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"google.golang.org/grpc"
	"io"
	"math/rand"
	"sync"
	"time"
)

// NewPartition creates a new proxy partition
func NewPartition(p *cluster.Partition) *Partition {
	partition := &Partition{
		Partition: p,
	}
	partition.Session = NewSession(partition)
	return partition
}

// PartitionID is a partition identifier
type PartitionID int

// Partition is a proxy partition
type Partition struct {
	*cluster.Partition
	*Session
	ID     PartitionID
	conn   *grpc.ClientConn
	client rsm.StorageServiceClient
	leader *cluster.Replica
	mu     sync.RWMutex
}

// doCommand submits a command to the service
func (p *Partition) doCommand(ctx context.Context, name string, input []byte, service rsm.ServiceId, context rsm.SessionCommandContext) ([]byte, rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	sessionRequest := &rsm.SessionRequest{
		Request: &rsm.SessionRequest_Command{
			Command: &rsm.SessionCommandRequest{
				Context: context,
				Command: rsm.ServiceCommandRequest{
					Service: service,
					Request: &rsm.ServiceCommandRequest_Operation{
						Operation: &rsm.ServiceOperationRequest{
							Method: name,
							Value:  input,
						},
					},
				},
			},
		},
	}

	request := &rsm.StorageRequest{
		PartitionID: uint32(p.ID),
		Request:     sessionRequest,
	}

	stream, err := p.client.Request(ctx, request)
	if err != nil {
		return nil, rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	// Wait for the result
	response, err := stream.Recv()
	if err != nil {
		return nil, rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	commandResponse := response.Response.GetCommand()
	return commandResponse.Response.GetOperation().Result, response.Response.Status, commandResponse.Context, nil
}

// doCommandStream submits a streaming command to the service
func (p *Partition) doCommandStream(ctx context.Context, name string, input []byte, service rsm.ServiceId, context rsm.SessionCommandContext, stream streams.WriteStream) error {
	sessionRequest := &rsm.SessionRequest{
		Request: &rsm.SessionRequest_Command{
			Command: &rsm.SessionCommandRequest{
				Context: context,
				Command: rsm.ServiceCommandRequest{
					Service: service,
					Request: &rsm.ServiceCommandRequest_Operation{
						Operation: &rsm.ServiceOperationRequest{
							Method: name,
							Value:  input,
						},
					},
				},
			},
		},
	}

	request := &rsm.StorageRequest{
		PartitionID: uint32(p.ID),
		Request:     sessionRequest,
	}

	go p.sendCommandStream(ctx, request, stream, false)
	return nil
}

// sendCommandStream submits a command to the service recursively
func (p *Partition) sendCommandStream(ctx context.Context, request *rsm.StorageRequest, stream streams.WriteStream, open bool) error {
	responseStream, err := p.client.Request(ctx, request)
	if err != nil {
		stream.Error(err)
		stream.Close()
		return err
	}

	for {
		response, err := responseStream.Recv()
		if err == io.EOF {
			stream.Close()
			return nil
		} else if err != nil {
			go p.sendCommandStream(ctx, request, stream, open)
		} else {
			commandResponse := response.Response.GetCommand()
			var result []byte
			if commandResponse.Response.GetOperation() != nil {
				result = commandResponse.Response.GetOperation().Result
			}

			switch response.Response.Type {
			case rsm.SessionResponseType_OPEN_STREAM:
				if !open {
					stream.Value(PartitionOutput{
						Type:    response.Response.Type,
						Status:  response.Response.Status,
						Context: commandResponse.Context,
						Result: streams.Result{
							Value: result,
						},
					})
					open = true
				}
			case rsm.SessionResponseType_CLOSE_STREAM:
				stream.Close()
				return nil
			case rsm.SessionResponseType_RESPONSE:
				stream.Value(PartitionOutput{
					Context: commandResponse.Context,
					Result: streams.Result{
						Value: result,
					},
				})
			}
		}
	}
}

// doQuery submits a query to the service
func (p *Partition) doQuery(ctx context.Context, name string, input []byte, service rsm.ServiceId, context rsm.SessionQueryContext) ([]byte, rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	sessionRequest := &rsm.SessionRequest{
		Request: &rsm.SessionRequest_Query{
			Query: &rsm.SessionQueryRequest{
				Context: context,
				Query: rsm.ServiceQueryRequest{
					Service: &service,
					Request: &rsm.ServiceQueryRequest_Operation{
						Operation: &rsm.ServiceOperationRequest{
							Method: name,
							Value:  input,
						},
					},
				},
			},
		},
	}

	request := &rsm.StorageRequest{
		PartitionID: uint32(p.ID),
		Request:     sessionRequest,
	}

	stream, err := p.client.Request(ctx, request)
	if err != nil {
		return nil, rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	// Wait for the result
	response, err := stream.Recv()
	if err != nil {
		return nil, rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	return response.Response.GetQuery().Response.GetOperation().Result, response.Response.Status, response.Response.GetQuery().Context, nil
}

// doQueryStream submits a streaming query to the service
func (p *Partition) doQueryStream(ctx context.Context, name string, input []byte, service rsm.ServiceId, context rsm.SessionQueryContext, stream streams.WriteStream) error {
	sessionRequest := &rsm.SessionRequest{
		Request: &rsm.SessionRequest_Query{
			Query: &rsm.SessionQueryRequest{
				Context: context,
				Query: rsm.ServiceQueryRequest{
					Service: &service,
					Request: &rsm.ServiceQueryRequest_Operation{
						Operation: &rsm.ServiceOperationRequest{
							Method: name,
							Value:  input,
						},
					},
				},
			},
		},
	}

	request := &rsm.StorageRequest{
		PartitionID: uint32(p.ID),
		Request:     sessionRequest,
	}

	go p.sendQueryStream(ctx, request, stream, false)
	return nil
}

// sendQueryStream submits a query to the service recursively
func (p *Partition) sendQueryStream(ctx context.Context, request *rsm.StorageRequest, stream streams.WriteStream, open bool) error {
	responseStream, err := p.client.Request(ctx, request)
	if err != nil {
		stream.Error(err)
		stream.Close()
		return err
	}

	for {
		response, err := responseStream.Recv()
		if err == io.EOF {
			stream.Close()
			return nil
		} else if err != nil {
			go p.sendQueryStream(ctx, request, stream, open)
		} else {
			queryResponse := response.Response.GetQuery()
			var result []byte
			if queryResponse.Response.GetOperation() != nil {
				result = queryResponse.Response.GetOperation().Result
			}

			switch response.Response.Type {
			case rsm.SessionResponseType_OPEN_STREAM:
				if !open {
					stream.Value(PartitionOutput{
						Type:    response.Response.Type,
						Status:  response.Response.Status,
						Context: queryResponse.Context,
						Result: streams.Result{
							Value: result,
						},
					})
					open = true
				}
			case rsm.SessionResponseType_CLOSE_STREAM:
				stream.Close()
				return nil
			case rsm.SessionResponseType_RESPONSE:
				stream.Value(PartitionOutput{
					Type:    response.Response.Type,
					Status:  response.Response.Status,
					Context: queryResponse.Context,
					Result: streams.Result{
						Value: result,
					},
				})
			}
		}
	}
}

// doMetadata submits a metadata query to the service
func (p *Partition) doMetadata(ctx context.Context, serviceType string, namespace string, context rsm.SessionQueryContext) ([]*rsm.ServiceId, rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	sessionRequest := &rsm.SessionRequest{
		Request: &rsm.SessionRequest_Query{
			Query: &rsm.SessionQueryRequest{
				Context: context,
				Query: rsm.ServiceQueryRequest{
					Request: &rsm.ServiceQueryRequest_Metadata{
						Metadata: &rsm.ServiceMetadataRequest{
							Type:      serviceType,
							Namespace: namespace,
						},
					},
				},
			},
		},
	}

	request := &rsm.StorageRequest{
		PartitionID: uint32(p.ID),
		Request:     sessionRequest,
	}

	stream, err := p.client.Request(ctx, request)
	if err != nil {
		return nil, rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	// Wait for the result
	response, err := stream.Recv()
	if err != nil {
		return nil, rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	return response.Response.GetQuery().Response.GetMetadata().Services, response.Response.Status, response.Response.GetQuery().Context, nil
}

// doOpenSession opens a new session
func (p *Partition) doOpenSession(ctx context.Context, context rsm.SessionCommandContext, timeout *time.Duration) (rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	sessionRequest := &rsm.SessionRequest{
		Request: &rsm.SessionRequest_OpenSession{
			OpenSession: &rsm.OpenSessionRequest{
				Timeout: timeout,
			},
		},
	}

	request := &rsm.StorageRequest{
		PartitionID: uint32(p.ID),
		Request:     sessionRequest,
	}

	stream, err := p.client.Request(ctx, request)
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	// Wait for the result
	response, err := stream.Recv()
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	sessionID := response.Response.GetOpenSession().SessionID
	return response.Response.Status, rsm.SessionResponseContext{
		SessionID: sessionID,
		StreamID:  sessionID,
		Index:     sessionID,
	}, nil
}

// doKeepAliveSession keeps a session alive
func (p *Partition) doKeepAliveSession(ctx context.Context, context rsm.SessionCommandContext, streams []rsm.SessionStreamContext) (rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	sessionRequest := &rsm.SessionRequest{
		Request: &rsm.SessionRequest_KeepAlive{
			KeepAlive: &rsm.KeepAliveRequest{
				SessionID:       context.SessionID,
				CommandSequence: context.SequenceNumber,
				Streams:         streams,
			},
		},
	}

	request := &rsm.StorageRequest{
		PartitionID: uint32(p.ID),
		Request:     sessionRequest,
	}

	stream, err := p.client.Request(ctx, request)
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	// Wait for the result
	response, err := stream.Recv()
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	return response.Response.Status, rsm.SessionResponseContext{SessionID: context.SessionID}, nil
}

// doCloseSession closes a session
func (p *Partition) doCloseSession(ctx context.Context, context rsm.SessionCommandContext) (rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	sessionRequest := &rsm.SessionRequest{
		Request: &rsm.SessionRequest_CloseSession{
			CloseSession: &rsm.CloseSessionRequest{
				SessionID: context.SessionID,
			},
		},
	}

	request := &rsm.StorageRequest{
		PartitionID: uint32(p.ID),
		Request:     sessionRequest,
	}

	stream, err := p.client.Request(ctx, request)
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	// Wait for the result
	response, err := stream.Recv()
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	return response.Response.Status, rsm.SessionResponseContext{SessionID: context.SessionID}, nil
}

// doCreateService creates the service
func (p *Partition) doCreateService(ctx context.Context, service rsm.ServiceId, context rsm.SessionCommandContext) (rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	sessionRequest := &rsm.SessionRequest{
		Request: &rsm.SessionRequest_Command{
			Command: &rsm.SessionCommandRequest{
				Context: context,
				Command: rsm.ServiceCommandRequest{
					Service: service,
					Request: &rsm.ServiceCommandRequest_Create{
						Create: &rsm.ServiceCreateRequest{},
					},
				},
			},
		},
	}

	request := &rsm.StorageRequest{
		PartitionID: uint32(p.ID),
		Request:     sessionRequest,
	}

	stream, err := p.client.Request(ctx, request)
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	// Wait for the result
	response, err := stream.Recv()
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	return response.Response.Status, response.Response.GetCommand().Context, nil
}

// doCloseService closes the service
func (p *Partition) doCloseService(ctx context.Context, service rsm.ServiceId, context rsm.SessionCommandContext) (rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	sessionRequest := &rsm.SessionRequest{
		Request: &rsm.SessionRequest_Command{
			Command: &rsm.SessionCommandRequest{
				Context: context,
				Command: rsm.ServiceCommandRequest{
					Service: service,
					Request: &rsm.ServiceCommandRequest_Close{
						Close: &rsm.ServiceCloseRequest{},
					},
				},
			},
		},
	}

	request := &rsm.StorageRequest{
		PartitionID: uint32(p.ID),
		Request:     sessionRequest,
	}

	stream, err := p.client.Request(ctx, request)
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	// Wait for the result
	response, err := stream.Recv()
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	return response.Response.Status, response.Response.GetCommand().Context, nil
}

// doDeleteService deletes the service
func (p *Partition) doDeleteService(ctx context.Context, service rsm.ServiceId, context rsm.SessionCommandContext) (rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	sessionRequest := &rsm.SessionRequest{
		Request: &rsm.SessionRequest_Command{
			Command: &rsm.SessionCommandRequest{
				Context: context,
				Command: rsm.ServiceCommandRequest{
					Service: service,
					Request: &rsm.ServiceCommandRequest_Delete{
						Delete: &rsm.ServiceDeleteRequest{},
					},
				},
			},
		},
	}

	request := &rsm.StorageRequest{
		PartitionID: uint32(p.ID),
		Request:     sessionRequest,
	}

	stream, err := p.client.Request(ctx, request)
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	// Wait for the result
	response, err := stream.Recv()
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}

	return response.Response.Status, response.Response.GetCommand().Context, nil
}

func (p *Partition) Connect() error {
	conn, err := p.connect()
	if err != nil {
		return err
	}
	p.client = rsm.NewStorageServiceClient(conn)
	return p.Session.open(context.TODO())
}

func (p *Partition) Close() error {
	err := p.Session.close(context.TODO())
	_ = p.close()
	return err
}

// connect gets the connection to the service
func (p *Partition) connect() (*grpc.ClientConn, error) {
	p.mu.RLock()
	conn := p.conn
	p.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	conn = p.conn
	if conn != nil {
		return conn, nil
	}

	if p.leader == nil {
		replicas := make([]*cluster.Replica, 0)
		for _, replica := range p.Replicas() {
			replicas = append(replicas, replica)
		}
		p.leader = replicas[rand.Intn(len(replicas))]
	}

	conn, err := p.leader.Connect(context.Background(), cluster.WithDialOption(grpc.WithInsecure()))
	if err != nil {
		return nil, err
	}
	p.conn = conn
	return conn, nil
}

// reconnect reconnects the client to the given leader if necessary
func (p *Partition) reconnect(leader *cluster.Replica) {
	if leader == nil {
		return
	}

	p.mu.RLock()
	connLeader := p.leader
	p.mu.RUnlock()
	if connLeader.ID == leader.ID {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.leader = leader
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
}

// close closes the connections
func (p *Partition) close() error {
	p.mu.Lock()
	conn := p.conn
	p.conn = nil
	p.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

// PartitionOutput is a result for session-supporting servers containing session header information
type PartitionOutput struct {
	streams.Result
	Type    rsm.SessionResponseType
	Status  rsm.SessionResponseStatus
	Context rsm.SessionResponseContext
}
