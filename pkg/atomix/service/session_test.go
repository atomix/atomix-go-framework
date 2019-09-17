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
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSessionService(t *testing.T) {
	context := &TestContext{}
	service := newTestService(context)

	ch := make(chan Result)
	go context.command(service, newOpenSession(), ch)
	out := <-ch
	assert.True(t, out.Succeeded())
	sessionID := getOpenSession(out.Value).SessionID

	setRequest1 := newCommand(sessionID, 1, "set", newSet("Hello world!"))
	ch1 := make(chan Result)
	go context.command(service, setRequest1, ch1)
	out1 := <-ch1
	assert.True(t, out1.Succeeded())

	setRequest3 := newCommand(sessionID, 3, "set", newSet("Hello world 3"))
	ch3 := make(chan Result)
	go context.command(service, setRequest3, ch3)

	time.Sleep(100 * time.Millisecond)

	setRequest2 := newCommand(sessionID, 2, "set", newSet("Hello world 2"))
	ch2 := make(chan Result)
	go context.command(service, setRequest2, ch2)

	out2 := <-ch2
	assert.True(t, out2.Succeeded())
	out3 := <-ch3
	assert.True(t, out3.Succeeded())
}

func newOpenSession() []byte {
	timeout := 30 * time.Second
	bytes, _ := proto.Marshal(&SessionRequest{
		Request: &SessionRequest_OpenSession{
			OpenSession: &OpenSessionRequest{
				Timeout: &timeout,
			},
		},
	})
	return bytes
}

func getOpenSession(bytes []byte) *OpenSessionResponse {
	sessionResponse := &SessionResponse{}
	_ = proto.Unmarshal(bytes, sessionResponse)
	return sessionResponse.GetOpenSession()
}

func newSet(value string) []byte {
	bytes, _ := proto.Marshal(&SetRequest{
		Value: value,
	})
	return bytes
}

func newCommand(sessionID uint64, commandID uint64, name string, bytes []byte) []byte {
	bytes, _ = proto.Marshal(&SessionRequest{
		Request: &SessionRequest_Command{
			Command: &SessionCommandRequest{
				Context: &SessionCommandContext{
					SessionID:      sessionID,
					SequenceNumber: commandID,
				},
				Name:  name,
				Input: bytes,
			},
		},
	})
	return bytes
}

type TestContext struct {
	index     uint64
	timestamp time.Time
	operation OperationType
}

func (c *TestContext) Node() string {
	return "test"
}

func (c *TestContext) Name() string {
	return "test"
}

func (c *TestContext) Namespace() string {
	return "test"
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

func (c *TestContext) command(service Service, input []byte, ch chan<- Result) {
	c.index++
	c.operation = OpTypeCommand
	service.Command(input, ch)
}
