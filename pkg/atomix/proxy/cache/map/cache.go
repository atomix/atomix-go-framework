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

package _map

import (
	"context"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"github.com/atomix/go-framework/pkg/atomix/time"
	"sync"
)

const Type = "Map"

// RegisterCache registers the cache on the given server
func RegisterCache(node proxy.Node) {
	node.PrimitiveTypes().RegisterCacheDecoratorFunc(Type, func(s interface{}) interface{} {
		return &CachedMap{
			server:  s.(_map.MapServiceServer),
			entries: make(map[string]*_map.Entry),
		}
	})
}

var log = logging.GetLogger("atomix", "map")

type CachedMap struct {
	server       _map.MapServiceServer
	entries      map[string]*_map.Entry
	maxTimestamp time.Timestamp
	mu           sync.RWMutex
}

func (s *CachedMap) Size(ctx context.Context, request *_map.SizeRequest) (*_map.SizeResponse, error) {
	return s.server.Size(ctx, request)
}

func (s *CachedMap) Put(ctx context.Context, request *_map.PutRequest) (*_map.PutResponse, error) {
	response, err := s.server.Put(ctx, request)
	if err != nil {
		return nil, errors.Proto(err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	timestamp := time.NewTimestamp(*response.Headers.Timestamp)
	if !timestamp.After(s.maxTimestamp) {
		return response, nil
	}

	s.maxTimestamp = timestamp
	entry, ok := s.entries[response.Entry.Key.Key]
	if !ok || meta.FromProto(response.Entry.Key.ObjectMeta).After(meta.FromProto(entry.Key.ObjectMeta)) {
		log.Debugf("Cached entry %v", response.Entry)
		s.entries[response.Entry.Key.Key] = &response.Entry
	}
	return response, nil
}

func (s *CachedMap) Get(ctx context.Context, request *_map.GetRequest) (*_map.GetResponse, error) {
	s.mu.RLock()
	entry, ok := s.entries[request.Key]
	s.mu.RUnlock()
	if ok {
		return &_map.GetResponse{
			Entry: *entry,
		}, nil
	}

	response, err := s.server.Get(ctx, request)
	if err != nil {
		return nil, errors.Proto(err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	timestamp := time.NewTimestamp(*response.Headers.Timestamp)
	if !timestamp.After(s.maxTimestamp) {
		return response, nil
	}

	s.maxTimestamp = timestamp
	entry, ok = s.entries[response.Entry.Key.Key]
	if !ok || meta.FromProto(response.Entry.Key.ObjectMeta).After(meta.FromProto(entry.Key.ObjectMeta)) {
		log.Debugf("Cached entry %v", response.Entry)
		s.entries[response.Entry.Key.Key] = &response.Entry
	}
	return response, nil
}

func (s *CachedMap) Remove(ctx context.Context, request *_map.RemoveRequest) (*_map.RemoveResponse, error) {
	response, err := s.server.Remove(ctx, request)
	if err != nil {
		return nil, errors.Proto(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	timestamp := time.NewTimestamp(*response.Headers.Timestamp)
	if !timestamp.After(s.maxTimestamp) {
		return response, nil
	}

	s.maxTimestamp = timestamp
	entry, ok := s.entries[response.Entry.Key.Key]
	if ok {
		log.Debugf("Evicted entry %+v", entry)
		delete(s.entries, response.Entry.Key.Key)
	}
	return response, nil
}

func (s *CachedMap) Clear(ctx context.Context, request *_map.ClearRequest) (*_map.ClearResponse, error) {
	response, err := s.server.Clear(ctx, request)
	if err != nil {
		return nil, errors.Proto(err)
	}
	s.mu.Lock()
	s.entries = make(map[string]*_map.Entry)
	s.mu.Unlock()
	log.Debugf("Cleared entries")
	return response, nil
}

func (s *CachedMap) Events(request *_map.EventsRequest, server _map.MapService_EventsServer) error {
	return s.server.Events(request, server)
}

func (s *CachedMap) Entries(request *_map.EntriesRequest, server _map.MapService_EntriesServer) error {
	return s.server.Entries(request, server)
}
