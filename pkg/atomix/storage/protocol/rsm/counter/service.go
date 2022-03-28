// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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

func (c *counterService) Set(set SetProposal) (*counter.SetResponse, error) {
	if err := checkPreconditions(c.value, set.Request().Preconditions); err != nil {
		return nil, err
	}
	c.value = set.Request().Value
	return &counter.SetResponse{
		Value: c.value,
	}, nil
}

func (c *counterService) Get(GetQuery) (*counter.GetResponse, error) {
	return &counter.GetResponse{
		Value: c.value,
	}, nil
}

func (c *counterService) Increment(increment IncrementProposal) (*counter.IncrementResponse, error) {
	c.value += increment.Request().Delta
	return &counter.IncrementResponse{
		Value: c.value,
	}, nil
}

func (c *counterService) Decrement(decrement DecrementProposal) (*counter.DecrementResponse, error) {
	c.value -= decrement.Request().Delta
	return &counter.DecrementResponse{
		Value: c.value,
	}, nil
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
