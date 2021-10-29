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
	mapapi "github.com/atomix/atomix-api/go/atomix/primitive/map"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/errors"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/logging"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/meta"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/time"
	"sync"
)

var log = logging.GetLogger("atomix", "map")

// NewServer creates a new cached map server
func NewServer(s mapapi.MapServiceServer) mapapi.MapServiceServer {
	return &Server{
		server:  s,
		entries: make(map[string]*mapapi.Entry),
	}
}

type Server struct {
	server       mapapi.MapServiceServer
	entries      map[string]*mapapi.Entry
	maxTimestamp time.Timestamp
	mu           sync.RWMutex
}

func (s *Server) Size(ctx context.Context, request *mapapi.SizeRequest) (*mapapi.SizeResponse, error) {
	return s.server.Size(ctx, request)
}

func (s *Server) Put(ctx context.Context, request *mapapi.PutRequest) (*mapapi.PutResponse, error) {
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

func (s *Server) Get(ctx context.Context, request *mapapi.GetRequest) (*mapapi.GetResponse, error) {
	s.mu.RLock()
	entry, ok := s.entries[request.Key]
	s.mu.RUnlock()
	if ok {
		return &mapapi.GetResponse{
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

func (s *Server) Remove(ctx context.Context, request *mapapi.RemoveRequest) (*mapapi.RemoveResponse, error) {
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

func (s *Server) Clear(ctx context.Context, request *mapapi.ClearRequest) (*mapapi.ClearResponse, error) {
	response, err := s.server.Clear(ctx, request)
	if err != nil {
		return nil, errors.Proto(err)
	}
	s.mu.Lock()
	s.entries = make(map[string]*mapapi.Entry)
	s.mu.Unlock()
	log.Debugf("Cleared entries")
	return response, nil
}

func (s *Server) Events(request *mapapi.EventsRequest, server mapapi.MapService_EventsServer) error {
	return s.server.Events(request, server)
}

func (s *Server) Entries(request *mapapi.EntriesRequest, server mapapi.MapService_EntriesServer) error {
	return s.server.Entries(request, server)
}
