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
	sm.Command(newOpenSession(), ch)
	out := <-ch
	assert.True(t, out.Succeeded())
}

func newOpenSession() []byte {
	bytes, _ := proto.Marshal(&SessionRequest{
		Request: &SessionRequest_OpenSession{
			OpenSession: &OpenSessionRequest{
				Timeout: int64(30 * time.Second),
			},
		},
	})
	return newTestRequest(bytes)
}

func newKeepAlive(sessionID uint64, commandID uint64, streams map[uint64]uint64) []byte {
	bytes, _ := proto.Marshal(&SessionRequest{
		Request: &SessionRequest_KeepAlive{
			KeepAlive: &KeepAliveRequest{
				SessionId:       sessionID,
				CommandSequence: commandID,
				Streams:         streams,
			},
		},
	})
	return newTestRequest(bytes)
}

func newCloseSession(sessionID uint64) []byte {
	bytes, _ := proto.Marshal(&SessionRequest{
		Request: &SessionRequest_CloseSession{
			CloseSession: &CloseSessionRequest{
				SessionId: sessionID,
			},
		},
	})
	return newTestRequest(bytes)
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
	return newTestRequest(bytes)
}

func newQuery(sessionID uint64, lastIndex uint64, lastCommandID uint64, name string, bytes []byte) []byte {
	bytes, _ = proto.Marshal(&SessionRequest{
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
	return newTestRequest(bytes)
}

func newTestRequest(bytes []byte) []byte {
	bytes, _ = proto.Marshal(&ServiceRequest{
		Id: &ServiceId{
			Type:      "test",
			Name:      "test",
			Namespace: "test",
		},
		Request: &ServiceRequest_Command{
			Command: bytes,
		},
	})
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
