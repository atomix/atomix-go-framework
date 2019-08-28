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
	"github.com/gogo/protobuf/proto"
)

func init() {
	service.RegisterService("counter", newService)
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
	c.Executor.Register("get", c.Get)
	c.Executor.Register("set", c.Set)
	c.Executor.Register("increment", c.Increment)
	c.Executor.Register("decrement", c.Decrement)
	c.Executor.Register("cas", c.CAS)
}

// Backup backs up the list service
func (c *Service) Backup() ([]byte, error) {
	snapshot := &CounterSnapshot{
		Value: c.value,
	}
	return proto.Marshal(snapshot)
}

// Restore restores the list service
func (c *Service) Restore(bytes []byte) error {
	snapshot := &CounterSnapshot{}
	if err := proto.Unmarshal(bytes, snapshot); err != nil {
		return err
	}
	c.value = snapshot.Value
	return nil
}

// Get gets the current value of the counter
func (c *Service) Get(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	ch <- c.NewResult(proto.Marshal(&GetResponse{
		Value: c.value,
	}))
}

// Set sets the value of the counter
func (c *Service) Set(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	request := &SetRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- c.NewFailure(err)
		return
	}

	c.value = request.Value
	ch <- c.NewResult(proto.Marshal(&SetResponse{}))
}

// Increment increments the value of the counter by a delta
func (c *Service) Increment(bytes []byte, ch chan<- service.Result) {
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

// Decrement decrements the value of the counter by a delta
func (c *Service) Decrement(bytes []byte, ch chan<- service.Result) {
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

// CAS updates the value of the counter if it matches a current value
func (c *Service) CAS(bytes []byte, ch chan<- service.Result) {
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
