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

package value

import (
	"github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/storage/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &valueService{
		Service: rsm.NewService(scheduler, context),
	}
}

// valueService is a state machine for a list primitive
type valueService struct {
	rsm.Service
	value   []byte
	version uint64
	streams []ServiceEventsStream
}

func (v *valueService) notify(event *value.EventsOutput) error {
	for _, stream := range v.streams {
		if err := stream.Notify(event); err != nil {
			return err
		}
	}
	return nil
}

func (v *valueService) Set(input *value.SetInput) (*value.SetOutput, error) {
	if input.ExpectVersion > 0 && input.ExpectVersion != v.version {
		return nil, errors.NewConflict("expected version %d does not match actual version %d", input.ExpectVersion, v.version)
	} else if input.ExpectValue != nil && len(input.ExpectValue) > 0 && (v.value == nil || !slicesEqual(v.value, input.ExpectValue)) {
		return nil, errors.NewConflict("expected value %v does not match actual value %v", input.ExpectValue, v.value)
	} else {
		prevValue := v.value
		prevVersion := v.version
		v.value = input.Value
		v.version++

		err := v.notify(&value.EventsOutput{
			Type:            value.EventsOutput_UPDATE,
			PreviousValue:   prevValue,
			PreviousVersion: prevVersion,
			NewValue:        v.value,
			NewVersion:      v.version,
		})
		if err != nil {
			return nil, err
		}
		return &value.SetOutput{
			Version: v.version,
		}, nil
	}
}

func (v *valueService) Get(input *value.GetInput) (*value.GetOutput, error) {
	return &value.GetOutput{
		Value:   v.value,
		Version: v.version,
	}, nil
}

func (v *valueService) Events(input *value.EventsInput, stream ServiceEventsStream) error {
	v.streams = append(v.streams, stream)
	return nil
}

func (v *valueService) Snapshot() (*value.Snapshot, error) {
	panic("implement me")
}

func (v *valueService) Restore(snapshot *value.Snapshot) error {
	panic("implement me")
}

func slicesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for _, i := range a {
		for _, j := range b {
			if i != j {
				return false
			}
		}
	}
	return true
}
