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
	"github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/storage/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &counterService{
		Service: rsm.NewService(scheduler, context),
	}
}

// counterService is a state machine for a counter primitive
type counterService struct {
	rsm.Service
	value int64
}

func (c *counterService) Set(input *counter.SetInput) (*counter.SetOutput, error) {
	prevValue := c.value
	c.value = input.Value
	return &counter.SetOutput{
		PreviousValue: prevValue,
	}, nil
}

func (c *counterService) Get(input *counter.GetInput) (*counter.GetOutput, error) {
	return &counter.GetOutput{
		Value: c.value,
	}, nil
}

func (c *counterService) Increment(input *counter.IncrementInput) (*counter.IncrementOutput, error) {
	prevValue := c.value
	c.value += input.Delta
	return &counter.IncrementOutput{
		PreviousValue: prevValue,
		NextValue:     c.value,
	}, nil
}

func (c *counterService) Decrement(input *counter.DecrementInput) (*counter.DecrementOutput, error) {
	prevValue := c.value
	c.value -= input.Delta
	return &counter.DecrementOutput{
		PreviousValue: prevValue,
		NextValue:     c.value,
	}, nil
}

func (c *counterService) CheckAndSet(input *counter.CheckAndSetInput) (*counter.CheckAndSetOutput, error) {
	if c.value != input.Expect {
		return nil, errors.NewConflict("optimistic lock failure")
	}
	c.value = input.Update
	return &counter.CheckAndSetOutput{}, nil
}

func (c *counterService) Snapshot() (*counter.Snapshot, error) {
	return &counter.Snapshot{
		Value: c.value,
	}, nil
}

func (c *counterService) Restore(snapshot *counter.Snapshot) error {
	c.value = snapshot.Value
	return nil
}
