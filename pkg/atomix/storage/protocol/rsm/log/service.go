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
	logapi "github.com/atomix/atomix-api/go/atomix/primitive/log"
)

func init() {
	registerServiceFunc(newService)
}

func newService(context ServiceContext) Service {
	return &logService{
		ServiceContext: context,
		indexes:        make(map[uint64]*LinkedEntry),
	}
}

// logService is a state machine for a log primitive
type logService struct {
	ServiceContext
	lastIndex  uint64
	indexes    map[uint64]*LinkedEntry
	firstEntry *LinkedEntry
	lastEntry  *LinkedEntry
}

func (l *logService) Backup(writer SnapshotWriter) error {
	return writer.WriteState(&LogState{})
}

func (l *logService) Restore(reader SnapshotReader) error {
	_, err := reader.ReadState()
	if err != nil {
		return err
	}
	return nil
}

func (l *logService) notify(event *logapi.EventsResponse) error {
	for _, events := range l.Proposals().Events().List() {
		if err := events.Notify(event); err != nil {
			return err
		}
	}
	return nil
}

func (l *logService) Size(size SizeProposal) error {
	return size.Reply(&logapi.SizeResponse{
		Size_: int32(len(l.indexes)),
	})
}

func (l *logService) Append(append AppendProposal) error {
	panic("implement me")
}

func (l *logService) Get(get GetProposal) error {
	panic("implement me")
}

func (l *logService) FirstEntry(firstEntry FirstEntryProposal) error {
	panic("implement me")
}

func (l *logService) LastEntry(lastEntry LastEntryProposal) error {
	panic("implement me")
}

func (l *logService) PrevEntry(prevEntry PrevEntryProposal) error {
	panic("implement me")
}

func (l *logService) NextEntry(nextEntry NextEntryProposal) error {
	panic("implement me")
}

func (l *logService) Remove(remove RemoveProposal) error {
	panic("implement me")
}

func (l *logService) Clear(clear ClearProposal) error {
	panic("implement me")
}

func (l *logService) Events(events EventsProposal) error {
	return nil
}

func (l *logService) Entries(entries EntriesProposal) error {
	panic("implement me")
}

// LinkedEntry is a doubly linked Entry
type LinkedEntry struct {
	*logapi.Entry
	Prev *LinkedEntry
	Next *LinkedEntry
}
