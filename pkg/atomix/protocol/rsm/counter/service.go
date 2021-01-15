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
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
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
	if err := checkPreconditions(c.value, input.Preconditions); err != nil {
		return nil, err
	}
	c.value = input.Value
	return &counter.SetOutput{
		Value: c.value,
	}, nil
}

func (c *counterService) Get(input *counter.GetInput) (*counter.GetOutput, error) {
	return &counter.GetOutput{
		Value: c.value,
	}, nil
}

func (c *counterService) Increment(input *counter.IncrementInput) (*counter.IncrementOutput, error) {
	c.value += input.Delta
	return &counter.IncrementOutput{
		Value: c.value,
	}, nil
}

func (c *counterService) Decrement(input *counter.DecrementInput) (*counter.DecrementOutput, error) {
	c.value -= input.Delta
	return &counter.DecrementOutput{
		Value: c.value,
	}, nil
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

func checkPreconditions(value int64, preconditions []counter.Precondition) error {
	for _, precondition := range preconditions {
		switch p := precondition.Precondition.(type) {
		case *counter.Precondition_Value:
			if value != p.Value {
				return errors.NewConflict("value precondition failed")
			}
		}
	}
	return nil
}
