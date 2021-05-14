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
	"github.com/atomix/atomix-api/go/atomix/primitive/indexedmap"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(context ServiceContext) Service {
	return &indexedMapService{
		ServiceContext: context,
		entries: make(map[string]*LinkedMapEntryValue),
		indexes: make(map[uint64]*LinkedMapEntryValue),
		timers:  make(map[string]rsm.Timer),
	}
}

// indexedMapService is a state machine for a map primitive
type indexedMapService struct {
	ServiceContext
	lastIndex  uint64
	entries    map[string]*LinkedMapEntryValue
	indexes    map[uint64]*LinkedMapEntryValue
	firstEntry *LinkedMapEntryValue
	lastEntry  *LinkedMapEntryValue
	timers     map[string]rsm.Timer
}

func (m *indexedMapService) SetState(state *IndexedMapState) error {
	return nil
}

func (m *indexedMapService) GetState() (*IndexedMapState, error) {
	return &IndexedMapState{}, nil
}

func (m *indexedMapService) Size(size SizeProposal) error {
	panic("implement me")
}

func (m *indexedMapService) Put(put PutProposal) error {
	panic("implement me")
}

func (m *indexedMapService) Get(get GetProposal) error {
	panic("implement me")
}

func (m *indexedMapService) FirstEntry(firstEntry FirstEntryProposal) error {
	panic("implement me")
}

func (m *indexedMapService) LastEntry(lastEntry LastEntryProposal) error {
	panic("implement me")
}

func (m *indexedMapService) PrevEntry(prevEntry PrevEntryProposal) error {
	panic("implement me")
}

func (m *indexedMapService) NextEntry(nextEntry NextEntryProposal) error {
	panic("implement me")
}

func (m *indexedMapService) Remove(remove RemoveProposal) error {
	panic("implement me")
}

func (m *indexedMapService) Clear(clear ClearProposal) error {
	panic("implement me")
}

func (m *indexedMapService) Events(events EventsProposal) error {
	return nil
}

func (m *indexedMapService) Entries(entries EntriesProposal) error {
	panic("implement me")
}

// LinkedMapEntryValue is a doubly linked MapEntryValue
type LinkedMapEntryValue struct {
	*indexedmap.Entry
	Prev *LinkedMapEntryValue
	Next *LinkedMapEntryValue
}
