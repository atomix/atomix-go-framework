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

package _map

import (
	"bytes"
	_map "github.com/atomix/atomix-api/go/atomix/primitive/map"
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	cluster "github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type testContext struct {
	index     rsm.Index
	timestamp time.Time
}

func (c *testContext) NodeID() string {
	return "node-1"
}

func (c *testContext) PartitionID() rsm.PartitionID {
	return 1
}

func (c *testContext) Index() rsm.Index {
	return c.index
}

func (c *testContext) Timestamp() time.Time {
	return c.timestamp
}

func (c *testContext) tick() {
	c.index++
	c.timestamp = time.Now()
}

func TestService(t *testing.T) {
	cluster := cluster.NewCluster(
		cluster.NewLocalNetwork(),
		protocolapi.ProtocolConfig{},
		cluster.WithMemberID("test"),
		cluster.WithNodeID("test"))
	reg := rsm.NewRegistry()
	reg.Register(Type, newServiceFunc)
	ctx := &testContext{}
	mgr := rsm.NewManager(cluster, reg)

	openSessionRequest := &rsm.StateMachineRequest{
		Timestamp: time.Now(),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_OpenSession{
				OpenSession: &rsm.OpenSessionRequest{},
			},
		},
	}
	openSessionBytes, err := proto.Marshal(openSessionRequest)
	assert.NoError(t, err)
	stream := streams.NewBufferedStream()
	ctx.tick()
	mgr.Command(openSessionBytes, stream)
	result, ok := stream.Receive()
	assert.True(t, ok)
	assert.True(t, result.Succeeded())

	sessionID := ctx.Index()
	createRequest := &rsm.StateMachineRequest{
		Timestamp: time.Now(),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_Command{
				Command: &rsm.SessionCommandRequest{
					Context: rsm.SessionCommandContext{
						SessionID: uint64(sessionID),
						RequestID: 1,
					},
					Command: rsm.ServiceCommandRequest{
						Service: rsm.ServiceId{
							Type: Type,
							Name: "test",
						},
						Request: &rsm.ServiceCommandRequest_Create{
							Create: &rsm.ServiceCreateRequest{},
						},
					},
				},
			},
		},
	}
	createBytes, err := proto.Marshal(createRequest)
	assert.NoError(t, err)
	stream = streams.NewBufferedStream()
	ctx.tick()
	mgr.Command(createBytes, stream)
	result, ok = stream.Receive()
	assert.True(t, ok)
	assert.True(t, result.Succeeded())

	buf := &bytes.Buffer{}
	err = mgr.Snapshot(buf)
	assert.NoError(t, err)

	err = mgr.Install(buf)
	assert.NoError(t, err)

	listenRequest := &_map.EventsRequest{}
	listenBytes, err := proto.Marshal(listenRequest)
	assert.NoError(t, err)

	eventsRequest := &rsm.StateMachineRequest{
		Timestamp: time.Now(),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_Command{
				Command: &rsm.SessionCommandRequest{
					Context: rsm.SessionCommandContext{
						SessionID: uint64(sessionID),
						RequestID: 2,
					},
					Command: rsm.ServiceCommandRequest{
						Service: rsm.ServiceId{
							Type: Type,
							Name: "test",
						},
						Request: &rsm.ServiceCommandRequest_Operation{
							Operation: &rsm.ServiceOperationRequest{
								Method: eventsOp,
								Value:  listenBytes,
							},
						},
					},
				},
			},
		},
	}
	eventsBytes, err := proto.Marshal(eventsRequest)
	assert.NoError(t, err)
	stream = streams.NewBufferedStream()
	ctx.tick()
	mgr.Command(eventsBytes, stream)
	result, ok = stream.Receive()
	assert.True(t, ok)
	assert.True(t, result.Succeeded())

	buf = &bytes.Buffer{}
	err = mgr.Snapshot(buf)
	assert.NoError(t, err)

	err = mgr.Install(buf)
	assert.NoError(t, err)
}
