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

package _map //nolint:golint

import (
	"github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/storage/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &mapService{
		Service: rsm.NewService(scheduler, context),
		entries: make(map[string]*_map.Entry),
		timers:  make(map[string]rsm.Timer),
	}
}

// mapService is a state machine for a map primitive
type mapService struct {
	rsm.Service
	entries map[string]*_map.Entry
	timers  map[string]rsm.Timer
	streams []ServiceEventsStream
}

func (m *mapService) notify(event *_map.EventsOutput) error {
	for _, stream := range m.streams {
		if err := stream.Notify(event); err != nil {
			return err
		}
	}
	return nil
}

func (m *mapService) Size() (*_map.SizeOutput, error) {
	return &_map.SizeOutput{
		Size_: uint32(len(m.entries)),
	}, nil
}

func (m *mapService) Exists(input *_map.ExistsInput) (*_map.ExistsOutput, error) {
	panic("implement me")
}

func (m *mapService) Put(input *_map.PutInput) (*_map.PutOutput, error) {
	panic("implement me")
}

func (m *mapService) Get(input *_map.GetInput) (*_map.GetOutput, error) {
	panic("implement me")
}

func (m *mapService) Remove(input *_map.RemoveInput) (*_map.RemoveOutput, error) {
	panic("implement me")
}

func (m *mapService) Clear() error {
	panic("implement me")
}

func (m *mapService) Events(input *_map.EventsInput, stream ServiceEventsStream) error {
	panic("implement me")
}

func (m *mapService) Entries(input *_map.EntriesInput, stream ServiceEntriesStream) error {
	panic("implement me")
}

func (m *mapService) Snapshot(writer ServiceSnapshotWriter) error {
	panic("implement me")
}

func (m *mapService) Restore(entry *_map.SnapshotEntry) error {
	panic("implement me")
}
