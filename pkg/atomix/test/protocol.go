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

package test

import (
	"context"
	"github.com/atomix/atomix-api/proto/atomix/controller"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"time"
)

func NewTestNode() *atomix.Node {
	node := atomix.NewNode("test", &controller.PartitionConfig{}, NewTestProtocol())
	go node.Start()
	return node
}

func NewTestProtocol() service.Protocol {
	return &TestProtocol{}
}

type TestProtocol struct {
	service.Protocol
	stateMachine service.StateMachine
	client       *TestClient
	context      *TestContext
}

func (p *TestProtocol) Start(cluster cluster.Cluster, registry *service.Registry) error {
	p.context = &TestContext{}
	p.stateMachine = service.NewPrimitiveStateMachine(registry, p.context)
	p.client = &TestClient{
		stateMachine: p.stateMachine,
		context:      p.context,
		ch:           make(chan testRequest),
	}
	p.client.start()
	return nil
}

func (p *TestProtocol) Client() service.Client {
	return p.client
}

func (p *TestProtocol) Stop() error {
	p.client.stop()
	return nil
}

type TestContext struct {
	service.Context
	index     uint64
	timestamp time.Time
	operation service.OperationType
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

type TestClient struct {
	stateMachine service.StateMachine
	context      *TestContext
	ch           chan testRequest
}

type testRequest struct {
	op    service.OperationType
	input []byte
	ch    chan<- service.Output
}

func (c *TestClient) start() {
	go c.processRequests()
}

func (c *TestClient) stop() {
	close(c.ch)
}

func (c *TestClient) processRequests() {
	for request := range c.ch {
		if request.op == service.OpTypeCommand {
			c.context.index++
			c.context.timestamp = time.Now()
			c.context.operation = service.OpTypeCommand
			c.stateMachine.Command(request.input, request.ch)
		} else {
			c.context.operation = service.OpTypeQuery
			c.stateMachine.Query(request.input, request.ch)
		}
	}
}

func (c *TestClient) Write(ctx context.Context, input []byte, ch chan<- service.Output) error {
	c.ch <- testRequest{
		op:    service.OpTypeCommand,
		input: input,
		ch:    ch,
	}
	return nil
}

func (c *TestClient) Read(ctx context.Context, input []byte, ch chan<- service.Output) error {
	c.ch <- testRequest{
		op:    service.OpTypeQuery,
		input: input,
		ch:    ch,
	}
	return nil
}
