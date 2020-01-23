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

package node

import (
	"github.com/atomix/go-framework/pkg/atomix/service"
	"github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPrimitiveStateMachine(t *testing.T) {
	ctx := &TestContext{}
	sm := NewPrimitiveStateMachine(GetRegistry(), ctx)

	ch := make(chan stream.Result)
	go ctx.command(sm, newOpenSessionRequest(t), ch)
	out := <-ch
	assert.True(t, out.Succeeded())
	openSessionResponse := getOpenSessionResponse(t, out.Value)
	assert.NotEqual(t, 0, openSessionResponse.SessionID)
	sessionID := openSessionResponse.SessionID

	ch = make(chan stream.Result)
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

	ch = make(chan stream.Result)
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
	timeout := 30 * time.Second
	bytes, err := proto.Marshal(&service.SessionRequest{
		Request: &service.SessionRequest_OpenSession{
			OpenSession: &service.OpenSessionRequest{
				Timeout: &timeout,
			},
		},
	})
	assert.NoError(t, err)
	return newTestCommandRequest(t, bytes)
}

func getOpenSessionResponse(t *testing.T, bytes []byte) *service.OpenSessionResponse {
	serviceResponse := &service.ServiceResponse{}
	assert.NoError(t, proto.Unmarshal(bytes, serviceResponse))
	sessionResponse := &service.SessionResponse{}
	assert.NoError(t, proto.Unmarshal(serviceResponse.GetCommand(), sessionResponse))
	return sessionResponse.GetOpenSession()
}

func newCommandRequest(t *testing.T, sessionID uint64, commandID uint64, name string, bytes []byte) []byte {
	bytes, err := proto.Marshal(&service.SessionRequest{
		Request: &service.SessionRequest_Command{
			Command: &service.SessionCommandRequest{
				Context: &service.SessionCommandContext{
					SessionID:      sessionID,
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

func getCommandResponse(t *testing.T, bytes []byte) *service.SessionCommandResponse {
	serviceResponse := &service.ServiceResponse{}
	assert.NoError(t, proto.Unmarshal(bytes, serviceResponse))
	sessionResponse := &service.SessionResponse{}
	assert.NoError(t, proto.Unmarshal(serviceResponse.GetCommand(), sessionResponse))
	return sessionResponse.GetCommand()
}

func newQueryRequest(t *testing.T, sessionID uint64, lastIndex uint64, lastCommandID uint64, name string, bytes []byte) []byte {
	bytes, err := proto.Marshal(&service.SessionRequest{
		Request: &service.SessionRequest_Query{
			Query: &service.SessionQueryRequest{
				Context: &service.SessionQueryContext{
					SessionID:          sessionID,
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

func getQueryResponse(t *testing.T, bytes []byte) *service.SessionQueryResponse {
	serviceResponse := &service.ServiceResponse{}
	assert.NoError(t, proto.Unmarshal(bytes, serviceResponse))
	sessionResponse := &service.SessionResponse{}
	assert.NoError(t, proto.Unmarshal(serviceResponse.GetQuery(), sessionResponse))
	return sessionResponse.GetQuery()
}

func newTestCommandRequest(t *testing.T, bytes []byte) []byte {
	bytes, err := proto.Marshal(&service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      "test",
			Name:      "test",
			Namespace: "test",
		},
		Request: &service.ServiceRequest_Command{
			Command: bytes,
		},
	})
	assert.NoError(t, err)
	return bytes
}

func newTestQueryRequest(t *testing.T, bytes []byte) []byte {
	bytes, err := proto.Marshal(&service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      "test",
			Name:      "test",
			Namespace: "test",
		},
		Request: &service.ServiceRequest_Query{
			Query: bytes,
		},
	})
	assert.NoError(t, err)
	return bytes
}

type TestContext struct {
	index     uint64
	timestamp time.Time
	operation service.OperationType
}

func (c *TestContext) Node() string {
	return "test"
}

func (c *TestContext) Index() uint64 {
	return c.index
}

func (c *TestContext) Timestamp() time.Time {
	return c.timestamp
}

func (c *TestContext) OperationType() service.OperationType {
	return c.operation
}

func (c *TestContext) command(sm StateMachine, input []byte, ch chan<- stream.Result) {
	c.index++
	c.operation = service.OpTypeCommand
	sm.Command(input, stream.NewChannelStream(ch))
}

func (c *TestContext) query(sm StateMachine, input []byte, ch chan<- stream.Result) {
	c.operation = service.OpTypeQuery
	sm.Query(input, stream.NewChannelStream(ch))
}
