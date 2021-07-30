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
	"github.com/atomix/atomix-api/go/atomix/primitive/counter"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
)

func init() {
	registerServiceFunc(newService)
}

func newService(context ServiceContext) Service {
	return &counterService{
		ServiceContext: context,
	}
}

// counterService is a state machine for a counter primitive
type counterService struct {
	ServiceContext
	value int64
}

func (c *counterService) Backup(writer SnapshotWriter) error {
	return writer.WriteState(&CounterState{
		Value: c.value,
	})
}

func (c *counterService) Restore(reader SnapshotReader) error {
	state, err := reader.ReadState()
	if err != nil {
		return err
	}
	c.value = state.Value
	return nil
}

func (c *counterService) Set(set SetProposal) error {
	request, err := set.Request()
	if err != nil {
		return err
	}
	if err := checkPreconditions(c.value, request.Preconditions); err != nil {
		return err
	}
	c.value = request.Value
	return set.Reply(&counter.SetResponse{
		Value: c.value,
	})
}

func (c *counterService) Get(get GetQuery) error {
	return get.Reply(&counter.GetResponse{
		Value: c.value,
	})
}

func (c *counterService) Increment(increment IncrementProposal) error {
	request, err := increment.Request()
	if err != nil {
		return err
	}
	c.value += request.Delta
	return increment.Reply(&counter.IncrementResponse{
		Value: c.value,
	})
}

func (c *counterService) Decrement(decrement DecrementProposal) error {
	request, err := decrement.Request()
	if err != nil {
		return err
	}
	c.value -= request.Delta
	return decrement.Reply(&counter.DecrementResponse{
		Value: c.value,
	})
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
