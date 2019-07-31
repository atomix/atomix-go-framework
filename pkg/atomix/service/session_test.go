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

	ch := make(chan Output)
	go context.command(service, newOpenSession(), ch)
	out := <-ch
	assert.True(t, out.Succeeded())
	sessionID := getOpenSession(out.Value).SessionId

	setRequest1 := newCommand(sessionID, 1, "set", newSet("Hello world!"))
	ch1 := make(chan Output)
	go context.command(service, setRequest1, ch1)
	out1 := <-ch1
	assert.True(t, out1.Succeeded())

	setRequest3 := newCommand(sessionID, 3, "set", newSet("Hello world 3"))
	ch3 := make(chan Output)
	go context.command(service, setRequest3, ch3)

	time.Sleep(100 * time.Millisecond)

	setRequest2 := newCommand(sessionID, 2, "set", newSet("Hello world 2"))
	ch2 := make(chan Output)
	go context.command(service, setRequest2, ch2)

	out2 := <-ch2
	assert.True(t, out2.Succeeded())
	out3 := <-ch3
	assert.True(t, out3.Succeeded())
}

func newOpenSession() []byte {
	bytes, _ := proto.Marshal(&SessionRequest{
		Request: &SessionRequest_OpenSession{
			OpenSession: &OpenSessionRequest{
				Timeout: int64(30 * time.Second),
			},
		},
	})
	return bytes
}

func getOpenSession(bytes []byte) *OpenSessionResponse {
	sessionResponse := &SessionResponse{}
	proto.Unmarshal(bytes, sessionResponse)
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
					SessionId:      sessionID,
					SequenceNumber: commandID,
				},
				Name:  name,
				Input: bytes,
			},
		},
	})
	return bytes
}

func getCommand(bytes []byte) *SessionCommandResponse {
	sessionResponse := &SessionResponse{}
	proto.Unmarshal(bytes, sessionResponse)
	return sessionResponse.GetCommand()
}

func getPut(bytes []byte) *SetResponse {
	setResponse := &SetResponse{}
	proto.Unmarshal(bytes, setResponse)
	return setResponse
}

func newGet() []byte {
	bytes, _ := proto.Marshal(&GetRequest{})
	return bytes
}

func newQuery(sessionID uint64, commandID uint64, index uint64, name string, bytes []byte) []byte {
	bytes, _ = proto.Marshal(&SessionRequest{
		Request: &SessionRequest_Query{
			Query: &SessionQueryRequest{
				Context: &SessionQueryContext{
					SessionId:          sessionID,
					LastIndex:          index,
					LastSequenceNumber: commandID,
				},
				Name:  name,
				Input: bytes,
			},
		},
	})
	return bytes
}

func getQuery(bytes []byte) *SessionQueryResponse {
	sessionResponse := &SessionResponse{}
	proto.Unmarshal(bytes, sessionResponse)
	return sessionResponse.GetQuery()
}

func getGet(bytes []byte) *GetResponse {
	getResponse := &GetResponse{}
	proto.Unmarshal(bytes, getResponse)
	return getResponse
}
