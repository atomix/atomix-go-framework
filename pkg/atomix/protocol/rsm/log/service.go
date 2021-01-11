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

package log

import (
	"github.com/atomix/api/go/atomix/primitive/log"
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &logService{
		Service: rsm.NewService(scheduler, context),
		indexes: make(map[uint64]*LinkedEntry),
	}
}

// logService is a state machine for a log primitive
type logService struct {
	rsm.Service
	lastIndex  uint64
	indexes    map[uint64]*LinkedEntry
	firstEntry *LinkedEntry
	lastEntry  *LinkedEntry
	streams    []ServiceEventsStream
}

func (l *logService) notify(event *log.EventsOutput) error {
	for _, stream := range l.streams {
		if err := stream.Notify(event); err != nil {
			return err
		}
	}
	return nil
}

func (l *logService) Size() (*log.SizeOutput, error) {
	return &log.SizeOutput{
		Size_: int32(len(l.indexes)),
	}, nil
}

func (l *logService) Exists(*log.ExistsInput) (*log.ExistsOutput, error) {
	panic("implement me")
}

func (l *logService) Append(*log.AppendInput) (*log.AppendOutput, error) {
	panic("implement me")
}

func (l *logService) Get(*log.GetInput) (*log.GetOutput, error) {
	panic("implement me")
}

func (l *logService) FirstEntry() (*log.FirstEntryOutput, error) {
	panic("implement me")
}

func (l *logService) LastEntry() (*log.LastEntryOutput, error) {
	panic("implement me")
}

func (l *logService) PrevEntry(*log.PrevEntryInput) (*log.PrevEntryOutput, error) {
	panic("implement me")
}

func (l *logService) NextEntry(*log.NextEntryInput) (*log.NextEntryOutput, error) {
	panic("implement me")
}

func (l *logService) Remove(*log.RemoveInput) (*log.RemoveOutput, error) {
	panic("implement me")
}

func (l *logService) Clear() error {
	panic("implement me")
}

func (l *logService) Events(*log.EventsInput, ServiceEventsStream) error {
	panic("implement me")
}

func (l *logService) Entries(*log.EntriesInput, ServiceEntriesStream) error {
	panic("implement me")
}

func (l *logService) Snapshot(ServiceSnapshotWriter) error {
	panic("implement me")
}

func (l *logService) Restore(*log.SnapshotEntry) error {
	panic("implement me")
}

// LinkedEntry is a doubly linked Entry
type LinkedEntry struct {
	*log.Entry
	Prev *LinkedEntry
	Next *LinkedEntry
}
