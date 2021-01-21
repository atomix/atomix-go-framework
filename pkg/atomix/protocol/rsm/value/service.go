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
	metaapi "github.com/atomix/api/go/atomix/primitive/meta"
	valueapi "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &valueService{
		Service: rsm.NewService(scheduler, context),
		streams: make(map[rsm.StreamID]ServiceEventsStream),
	}
}

// valueService is a state machine for a list primitive
type valueService struct {
	rsm.Service
	value   valueapi.Value
	streams map[rsm.StreamID]ServiceEventsStream
}

func (v *valueService) notify(event valueapi.Event) error {
	output := &valueapi.EventsResponse{
		Event: event,
	}
	for _, stream := range v.streams {
		if err := stream.Notify(output); err != nil {
			return err
		}
	}
	return nil
}

func (v *valueService) Set(input *valueapi.SetRequest) (*valueapi.SetResponse, error) {
	for _, precondition := range input.Preconditions {
		switch p := precondition.Precondition.(type) {
		case *valueapi.Precondition_Metadata:
			if !meta.Equal(v.value.ObjectMeta, *p.Metadata) {
				return nil, errors.NewConflict("metadata precondition failed")
			}
		}
	}

	meta := input.Value.ObjectMeta
	if meta.Revision == nil {
		if v.value.Revision != nil {
			meta.Revision = &metaapi.Revision{
				Num: v.value.Revision.Num + 1,
			}
		} else {
			meta.Revision = &metaapi.Revision{
				Num: 1,
			}
		}
	}

	value := valueapi.Value{
		ObjectMeta: meta,
		Value:      input.Value.Value,
	}
	v.value = value

	err := v.notify(valueapi.Event{
		Type:  valueapi.Event_UPDATE,
		Value: v.value,
	})
	if err != nil {
		return nil, err
	}
	return &valueapi.SetResponse{
		Value: value,
	}, nil
}

func (v *valueService) Get(input *valueapi.GetRequest) (*valueapi.GetResponse, error) {
	return &valueapi.GetResponse{
		Value: v.value,
	}, nil
}

func (v *valueService) Events(input *valueapi.EventsRequest, stream ServiceEventsStream) (rsm.StreamCloser, error) {
	v.streams[stream.ID()] = stream
	return func() {
		delete(v.streams, stream.ID())
	}, nil
}
