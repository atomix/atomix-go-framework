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
		streams: make(map[rsm.StreamID]ServiceEventsStream),
	}
}

// logService is a state machine for a log primitive
type logService struct {
	rsm.Service
	lastIndex  uint64
	indexes    map[uint64]*LinkedEntry
	firstEntry *LinkedEntry
	lastEntry  *LinkedEntry
	streams    map[rsm.StreamID]ServiceEventsStream
}

func (l *logService) notify(event *log.EventsResponse) error {
	for _, stream := range l.streams {
		if err := stream.Notify(event); err != nil {
			return err
		}
	}
	return nil
}

func (l *logService) Size(*log.SizeRequest) (*log.SizeResponse, error) {
	return &log.SizeResponse{
		Size_: int32(len(l.indexes)),
	}, nil
}

func (l *logService) Append(*log.AppendRequest) (*log.AppendResponse, error) {
	panic("implement me")
}

func (l *logService) Get(*log.GetRequest) (*log.GetResponse, error) {
	panic("implement me")
}

func (l *logService) FirstEntry(*log.FirstEntryRequest) (*log.FirstEntryResponse, error) {
	panic("implement me")
}

func (l *logService) LastEntry(*log.LastEntryRequest) (*log.LastEntryResponse, error) {
	panic("implement me")
}

func (l *logService) PrevEntry(*log.PrevEntryRequest) (*log.PrevEntryResponse, error) {
	panic("implement me")
}

func (l *logService) NextEntry(*log.NextEntryRequest) (*log.NextEntryResponse, error) {
	panic("implement me")
}

func (l *logService) Remove(*log.RemoveRequest) (*log.RemoveResponse, error) {
	panic("implement me")
}

func (l *logService) Clear(*log.ClearRequest) (*log.ClearResponse, error) {
	panic("implement me")
}

func (l *logService) Events(input *log.EventsRequest, stream ServiceEventsStream) (rsm.StreamCloser, error) {
	l.streams[stream.ID()] = stream
	return func() {
		delete(l.streams, stream.ID())
	}, nil
}

func (l *logService) Entries(*log.EntriesRequest, ServiceEntriesStream) (rsm.StreamCloser, error) {
	panic("implement me")
}

// LinkedEntry is a doubly linked Entry
type LinkedEntry struct {
	*log.Entry
	Prev *LinkedEntry
	Next *LinkedEntry
}
