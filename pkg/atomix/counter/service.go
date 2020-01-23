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

package counter

import (
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/service"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/gogo/protobuf/proto"
	"io"
)

func init() {
	node.RegisterService(counterType, newService)
}

// newService returns a new Service
func newService(context service.Context) service.Service {
	service := &Service{
		SimpleService: service.NewSimpleService(context),
	}
	service.init()
	return service
}

// Service is a state machine for a counter primitive
type Service struct {
	*service.SimpleService
	value int64
}

// init initializes the list service
func (c *Service) init() {
	c.Executor.RegisterUnaryOperation(opGet, c.Get)
	c.Executor.RegisterUnaryOperation(opSet, c.Set)
	c.Executor.RegisterUnaryOperation(opIncrement, c.Increment)
	c.Executor.RegisterUnaryOperation(opDecrement, c.Decrement)
	c.Executor.RegisterUnaryOperation(opCAS, c.CAS)
}

// Snapshot takes a snapshot of the service
func (c *Service) Snapshot(writer io.Writer) error {
	if err := c.SimpleService.Snapshot(writer); err != nil {
		return err
	}

	snapshot := &CounterSnapshot{
		Value: c.value,
	}
	bytes, err := proto.Marshal(snapshot)
	if err != nil {
		return err
	}
	return util.WriteBytes(writer, bytes)
}

// Install restores the service from a snapshot
func (c *Service) Install(reader io.Reader) error {
	if err := c.SimpleService.Install(reader); err != nil {
		return err
	}

	bytes, err := util.ReadBytes(reader)
	if err != nil {
		return err
	}

	snapshot := &CounterSnapshot{}
	if err := proto.Unmarshal(bytes, snapshot); err != nil {
		return err
	}
	c.value = snapshot.Value
	return nil
}

// Get gets the current value of the counter
func (c *Service) Get(bytes []byte) ([]byte, error) {
	return proto.Marshal(&GetResponse{
		Value: c.value,
	})
}

// Set sets the value of the counter
func (c *Service) Set(bytes []byte) ([]byte, error) {
	request := &SetRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	c.value = request.Value
	return proto.Marshal(&SetResponse{})
}

// Increment increments the value of the counter by a delta
func (c *Service) Increment(bytes []byte) ([]byte, error) {
	request := &IncrementRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	prevValue := c.value
	c.value += request.Delta
	return proto.Marshal(&IncrementResponse{
		PreviousValue: prevValue,
		NextValue:     c.value,
	})
}

// Decrement decrements the value of the counter by a delta
func (c *Service) Decrement(bytes []byte) ([]byte, error) {
	request := &DecrementRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	prevValue := c.value
	c.value -= request.Delta
	return proto.Marshal(&IncrementResponse{
		PreviousValue: prevValue,
		NextValue:     c.value,
	})
}

// CAS updates the value of the counter if it matches a current value
func (c *Service) CAS(bytes []byte) ([]byte, error) {
	request := &CheckAndSetRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	if c.value == request.Expect {
		c.value = request.Update
		return proto.Marshal(&CheckAndSetResponse{
			Succeeded: true,
		})
	}
	return proto.Marshal(&CheckAndSetResponse{
		Succeeded: false,
	})
}
