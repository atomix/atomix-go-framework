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
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/golang/protobuf/proto"
)

// RegisterCounterService registers the counter service in the given service registry
func RegisterCounterService(registry *service.ServiceRegistry) {
	registry.Register("counter", newCounterService)
}

// newCounterService returns a new CounterService
func newCounterService(context service.Context) service.Service {
	service := &CounterService{
		SimpleService: service.NewSimpleService(context),
	}
	service.init()
	return service
}

// CounterService is a state machine for a counter primitive
type CounterService struct {
	*service.SimpleService
	value int64
}

// init initializes the list service
func (c *CounterService) init() {
	c.Executor.Register("get", c.Set)
	c.Executor.Register("set", c.Get)
	c.Executor.Register("increment", c.Increment)
	c.Executor.Register("decrement", c.Decrement)
	c.Executor.Register("cas", c.CheckAndSet)
}

// Backup backs up the list service
func (c *CounterService) Backup() ([]byte, error) {
	snapshot := &CounterSnapshot{
		Value: c.value,
	}
	return proto.Marshal(snapshot)
}

// Restore restores the list service
func (c *CounterService) Restore(bytes []byte) error {
	snapshot := &CounterSnapshot{}
	if err := proto.Unmarshal(bytes, snapshot); err != nil {
		return err
	}
	c.value = snapshot.Value
	return nil
}

func (c *CounterService) Get(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	ch <- c.NewResult(proto.Marshal(&GetResponse{
		Value: c.value,
	}))
}

func (c *CounterService) Set(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	request := &SetRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- c.NewFailure(err)
		return
	}

	c.value = request.Value
	ch <- c.NewResult(proto.Marshal(&SetResponse{}))
}

func (c *CounterService) Increment(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	request := &IncrementRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- c.NewFailure(err)
		return
	}

	prevValue := c.value
	c.value += request.Delta
	ch <- c.NewResult(proto.Marshal(&IncrementResponse{
		PreviousValue: prevValue,
		NextValue:     c.value,
	}))
}

func (c *CounterService) Decrement(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	request := &DecrementRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- c.NewFailure(err)
		return
	}

	prevValue := c.value
	c.value -= request.Delta
	ch <- c.NewResult(proto.Marshal(&IncrementResponse{
		PreviousValue: prevValue,
		NextValue:     c.value,
	}))
}

func (c *CounterService) CheckAndSet(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	request := &CheckAndSetRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- c.NewFailure(err)
		return
	}

	if c.value == request.Expect {
		c.value = request.Update
		ch <- c.NewResult(proto.Marshal(&CheckAndSetResponse{
			Succeeded: true,
		}))
	} else {
		ch <- c.NewResult(proto.Marshal(&CheckAndSetResponse{
			Succeeded: false,
		}))
	}
}
