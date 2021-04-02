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

package indexedmap

import (
	"github.com/atomix/api/go/atomix/primitive/indexedmap"
	"github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &indexedMapService{
		Service: rsm.NewService(scheduler, context),
		entries: make(map[string]*LinkedMapEntryValue),
		indexes: make(map[uint64]*LinkedMapEntryValue),
		timers:  make(map[string]rsm.Timer),
		streams: make(map[rsm.StreamID]ServiceEventsStream),
	}
}

// indexedMapService is a state machine for a map primitive
type indexedMapService struct {
	rsm.Service
	lastIndex  uint64
	entries    map[string]*LinkedMapEntryValue
	indexes    map[uint64]*LinkedMapEntryValue
	firstEntry *LinkedMapEntryValue
	lastEntry  *LinkedMapEntryValue
	timers     map[string]rsm.Timer
	streams    map[rsm.StreamID]ServiceEventsStream
}

func (m *indexedMapService) Size(*indexedmap.SizeRequest) (*indexedmap.SizeResponse, error) {
	panic("implement me")
}

func (m *indexedMapService) Put(*indexedmap.PutRequest) (*indexedmap.PutResponse, error) {
	panic("implement me")
}

func (m *indexedMapService) Get(*indexedmap.GetRequest) (*indexedmap.GetResponse, error) {
	panic("implement me")
}

func (m *indexedMapService) FirstEntry(*indexedmap.FirstEntryRequest) (*indexedmap.FirstEntryResponse, error) {
	panic("implement me")
}

func (m *indexedMapService) LastEntry(*indexedmap.LastEntryRequest) (*indexedmap.LastEntryResponse, error) {
	panic("implement me")
}

func (m *indexedMapService) PrevEntry(*indexedmap.PrevEntryRequest) (*indexedmap.PrevEntryResponse, error) {
	panic("implement me")
}

func (m *indexedMapService) NextEntry(*indexedmap.NextEntryRequest) (*indexedmap.NextEntryResponse, error) {
	panic("implement me")
}

func (m *indexedMapService) Remove(*indexedmap.RemoveRequest) (*indexedmap.RemoveResponse, error) {
	panic("implement me")
}

func (m *indexedMapService) Clear(*indexedmap.ClearRequest) (*indexedmap.ClearResponse, error) {
	panic("implement me")
}

func (m *indexedMapService) Events(input *indexedmap.EventsRequest, stream ServiceEventsStream) (rsm.StreamCloser, error) {
	m.streams[stream.ID()] = stream
	return func() {
		delete(m.streams, stream.ID())
	}, nil
}

func (m *indexedMapService) Entries(*indexedmap.EntriesRequest, ServiceEntriesStream) (rsm.StreamCloser, error) {
	panic("implement me")
}

// LinkedMapEntryValue is a doubly linked MapEntryValue
type LinkedMapEntryValue struct {
	*indexedmap.Entry
	Prev *LinkedMapEntryValue
	Next *LinkedMapEntryValue
}
