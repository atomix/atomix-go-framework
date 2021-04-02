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
	"context"
	logapi "github.com/atomix/api/go/atomix/primitive/log"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
)

var log = logging.GetLogger("atomix", "map")

// NewReadOnlyLogServer creates a new read-only log server
func NewReadOnlyLogServer(s logapi.LogServiceServer) logapi.LogServiceServer {
	return &ReadOnlyLogServer{
		server: s,
	}
}

type ReadOnlyLogServer struct {
	server logapi.LogServiceServer
}

func (s *ReadOnlyLogServer) Size(ctx context.Context, request *logapi.SizeRequest) (*logapi.SizeResponse, error) {
	return s.server.Size(ctx, request)
}

func (s *ReadOnlyLogServer) Append(ctx context.Context, request *logapi.AppendRequest) (*logapi.AppendResponse, error) {
	return nil, errors.NewUnauthorized("Append operation is not permitted")
}

func (s *ReadOnlyLogServer) Get(ctx context.Context, request *logapi.GetRequest) (*logapi.GetResponse, error) {
	return s.server.Get(ctx, request)
}

func (s *ReadOnlyLogServer) FirstEntry(ctx context.Context, request *logapi.FirstEntryRequest) (*logapi.FirstEntryResponse, error) {
	return s.server.FirstEntry(ctx, request)
}

func (s *ReadOnlyLogServer) LastEntry(ctx context.Context, request *logapi.LastEntryRequest) (*logapi.LastEntryResponse, error) {
	return s.server.LastEntry(ctx, request)
}

func (s *ReadOnlyLogServer) PrevEntry(ctx context.Context, request *logapi.PrevEntryRequest) (*logapi.PrevEntryResponse, error) {
	return s.server.PrevEntry(ctx, request)
}

func (s *ReadOnlyLogServer) NextEntry(ctx context.Context, request *logapi.NextEntryRequest) (*logapi.NextEntryResponse, error) {
	return s.server.NextEntry(ctx, request)
}

func (s *ReadOnlyLogServer) Remove(ctx context.Context, request *logapi.RemoveRequest) (*logapi.RemoveResponse, error) {
	return nil, errors.NewUnauthorized("Remove operation is not permitted")
}

func (s *ReadOnlyLogServer) Clear(ctx context.Context, request *logapi.ClearRequest) (*logapi.ClearResponse, error) {
	return nil, errors.NewUnauthorized("Clear operation is not permitted")
}

func (s *ReadOnlyLogServer) Events(request *logapi.EventsRequest, server logapi.LogService_EventsServer) error {
	return s.server.Events(request, server)
}

func (s *ReadOnlyLogServer) Entries(request *logapi.EntriesRequest, server logapi.LogService_EntriesServer) error {
	return s.server.Entries(request, server)
}

var _ logapi.LogServiceServer = &ReadOnlyLogServer{}
