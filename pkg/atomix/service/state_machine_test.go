package service

import (
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPrimitiveStateMachine(t *testing.T) {
	ctx := &TestContext{}
	sm := NewPrimitiveStateMachine(getServiceRegistry(), ctx)

	ch := make(chan Output)
	go ctx.command(sm, newOpenSessionRequest(t), ch)
	out := <-ch
	assert.True(t, out.Succeeded())
	openSessionResponse := getOpenSessionResponse(t, out.Value)
	assert.NotEqual(t, 0, openSessionResponse.SessionId)
	sessionID := openSessionResponse.SessionId

	ch = make(chan Output)
	bytes, err := proto.Marshal(&SetRequest{
		Value: "Hello world!",
	})
	assert.NoError(t, err)
	go ctx.command(sm, newCommandRequest(t, sessionID, 1, "set", bytes), ch)
	out = <-ch
	assert.True(t, out.Succeeded())
	commandResponse := getCommandResponse(t, out.Value)
	setResponse := &SetResponse{}
	assert.NoError(t, proto.Unmarshal(commandResponse.Output, setResponse))

	ch = make(chan Output)
	bytes, err = proto.Marshal(&GetRequest{})
	assert.NoError(t, err)
	go ctx.query(sm, newQueryRequest(t, sessionID, commandResponse.Context.Index, 1, "get", bytes), ch)
	out = <-ch
	assert.True(t, out.Succeeded())
	queryResponse := getQueryResponse(t, out.Value)
	getResponse := &GetResponse{}
	assert.NoError(t, proto.Unmarshal(queryResponse.Output, getResponse))
	assert.Equal(t, "Hello world!", getResponse.Value)
}

func newOpenSessionRequest(t *testing.T) []byte {
	bytes, err := proto.Marshal(&SessionRequest{
		Request: &SessionRequest_OpenSession{
			OpenSession: &OpenSessionRequest{
				Timeout: int64(30 * time.Second),
			},
		},
	})
	assert.NoError(t, err)
	return newTestCommandRequest(t, bytes)
}

func getOpenSessionResponse(t *testing.T, bytes []byte) *OpenSessionResponse {
	serviceResponse := &ServiceResponse{}
	assert.NoError(t, proto.Unmarshal(bytes, serviceResponse))
	sessionResponse := &SessionResponse{}
	assert.NoError(t, proto.Unmarshal(serviceResponse.GetCommand(), sessionResponse))
	return sessionResponse.GetOpenSession()
}

func newKeepAliveRequest(t *testing.T, sessionID uint64, commandID uint64, streams map[uint64]uint64) []byte {
	bytes, err := proto.Marshal(&SessionRequest{
		Request: &SessionRequest_KeepAlive{
			KeepAlive: &KeepAliveRequest{
				SessionId:       sessionID,
				CommandSequence: commandID,
				Streams:         streams,
			},
		},
	})
	assert.NoError(t, err)
	return newTestCommandRequest(t, bytes)
}

func newCloseSessionRequest(t *testing.T, sessionID uint64) []byte {
	bytes, err := proto.Marshal(&SessionRequest{
		Request: &SessionRequest_CloseSession{
			CloseSession: &CloseSessionRequest{
				SessionId: sessionID,
			},
		},
	})
	assert.NoError(t, err)
	return newTestCommandRequest(t, bytes)
}

func newCommandRequest(t *testing.T, sessionID uint64, commandID uint64, name string, bytes []byte) []byte {
	bytes, err := proto.Marshal(&SessionRequest{
		Request: &SessionRequest_Command{
			Command: &SessionCommandRequest{
				Context: &SessionCommandContext{
					SessionId:      sessionID,
					SequenceNumber: commandID,
				},
				Name:  name,
				Input: bytes,
			},
		},
	})
	assert.NoError(t, err)
	return newTestCommandRequest(t, bytes)
}

func getCommandResponse(t *testing.T, bytes []byte) *SessionCommandResponse {
	serviceResponse := &ServiceResponse{}
	assert.NoError(t, proto.Unmarshal(bytes, serviceResponse))
	sessionResponse := &SessionResponse{}
	assert.NoError(t, proto.Unmarshal(serviceResponse.GetCommand(), sessionResponse))
	return sessionResponse.GetCommand()
}

func newQueryRequest(t *testing.T, sessionID uint64, lastIndex uint64, lastCommandID uint64, name string, bytes []byte) []byte {
	bytes, err := proto.Marshal(&SessionRequest{
		Request: &SessionRequest_Query{
			Query: &SessionQueryRequest{
				Context: &SessionQueryContext{
					SessionId:          sessionID,
					LastIndex:          lastIndex,
					LastSequenceNumber: lastCommandID,
				},
				Name:  name,
				Input: bytes,
			},
		},
	})
	assert.NoError(t, err)
	return newTestQueryRequest(t, bytes)
}

func getQueryResponse(t *testing.T, bytes []byte) *SessionQueryResponse {
	serviceResponse := &ServiceResponse{}
	assert.NoError(t, proto.Unmarshal(bytes, serviceResponse))
	sessionResponse := &SessionResponse{}
	assert.NoError(t, proto.Unmarshal(serviceResponse.GetQuery(), sessionResponse))
	return sessionResponse.GetQuery()
}

func newTestCommandRequest(t *testing.T, bytes []byte) []byte {
	bytes, err := proto.Marshal(&ServiceRequest{
		Id: &ServiceId{
			Type:      "test",
			Name:      "test",
			Namespace: "test",
		},
		Request: &ServiceRequest_Command{
			Command: bytes,
		},
	})
	assert.NoError(t, err)
	return bytes
}

func newTestQueryRequest(t *testing.T, bytes []byte) []byte {
	bytes, err := proto.Marshal(&ServiceRequest{
		Id: &ServiceId{
			Type:      "test",
			Name:      "test",
			Namespace: "test",
		},
		Request: &ServiceRequest_Query{
			Query: bytes,
		},
	})
	assert.NoError(t, err)
	return bytes
}

func getServiceRegistry() *ServiceRegistry {
	registry := NewServiceRegistry()
	RegisterTestService(registry)
	return registry
}

type TestContext struct {
	Context
	index     uint64
	timestamp time.Time
	operation OperationType
}

func (c *TestContext) Index() uint64 {
	return c.index
}

func (c *TestContext) Timestamp() time.Time {
	return c.timestamp
}

func (c *TestContext) OperationType() OperationType {
	return c.operation
}

func (c *TestContext) command(sm StateMachine, input []byte, ch chan<- Output) {
	c.index++
	c.operation = OpTypeCommand
	sm.Command(input, ch)
}

func (c *TestContext) query(sm StateMachine, input []byte, ch chan<- Output) {
	c.operation = OpTypeQuery
	sm.Query(input, ch)
}
