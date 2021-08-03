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
	metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	valueapi "github.com/atomix/atomix-api/go/atomix/primitive/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
)

func init() {
	registerServiceFunc(newService)
}

func newService(context ServiceContext) Service {
	return &valueService{
		ServiceContext: context,
	}
}

// valueService is a state machine for a list primitive
type valueService struct {
	ServiceContext
	value valueapi.Value
}

func (v *valueService) Backup(writer SnapshotWriter) error {
	return writer.WriteState(&ValueState{
		ObjectMeta: v.value.ObjectMeta,
		Value:      v.value.Value,
	})
}

func (v *valueService) Restore(reader SnapshotReader) error {
	state, err := reader.ReadState()
	if err != nil {
		return err
	}
	v.value = valueapi.Value{
		ObjectMeta: state.ObjectMeta,
		Value:      state.Value,
	}
	return nil
}

func (v *valueService) notify(event valueapi.Event) {
	output := &valueapi.EventsResponse{
		Event: event,
	}
	for _, events := range v.Proposals().Events().List() {
		events.Notify(output)
	}
}

func (v *valueService) Set(set SetProposal) (*valueapi.SetResponse, error) {
	for _, precondition := range set.Request().Preconditions {
		switch p := precondition.Precondition.(type) {
		case *valueapi.Precondition_Metadata:
			if !meta.Equal(v.value.ObjectMeta, *p.Metadata) {
				return nil, errors.NewConflict("metadata precondition failed")
			}
		}
	}

	meta := set.Request().Value.ObjectMeta
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
		Value:      set.Request().Value.Value,
	}
	v.value = value

	v.notify(valueapi.Event{
		Type:  valueapi.Event_UPDATE,
		Value: v.value,
	})
	return &valueapi.SetResponse{
		Value: value,
	}, nil
}

func (v *valueService) Get(GetQuery) (*valueapi.GetResponse, error) {
	return &valueapi.GetResponse{
		Value: v.value,
	}, nil
}

func (v *valueService) Events(events EventsProposal) {
	events.Notify(&valueapi.EventsResponse{})
}
