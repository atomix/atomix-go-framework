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

package session

import (
	"context"
	api "github.com/atomix/api/proto/atomix/session"
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/server"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func init() {
	node.RegisterServer(registerServer)
}

// registerServer registers a session management server with the given gRPC server
func registerServer(server *grpc.Server, protocol node.Protocol) {
	api.RegisterSessionServiceServer(server, newServer(protocol))
}

func newServer(protocol node.Protocol) api.SessionServiceServer {
	return &Server{
		Server: &server.Server{
			Protocol: protocol,
		},
	}
}

// Server is an implementation of SessionServiceServer for session management
type Server struct {
	*server.Server
}

// OpenSession opens a new session
func (s *Server) OpenSession(ctx context.Context, request *api.OpenSessionRequest) (*api.OpenSessionResponse, error) {
	log.Tracef("Received OpenSessionRequest %+v", request)
	header, err := s.DoOpenSession(ctx, request.Header, request.Timeout)
	if err != nil {
		return nil, err
	}
	response := &api.OpenSessionResponse{
		Header: header,
	}
	log.Tracef("Sending OpenSessionResponse %+v", response)
	return response, nil
}

// KeepAlive keeps a session alive
func (s *Server) KeepAlive(ctx context.Context, request *api.KeepAliveRequest) (*api.KeepAliveResponse, error) {
	log.Tracef("Received KeepAliveRequest %+v", request)
	header, err := s.DoKeepAliveSession(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	response := &api.KeepAliveResponse{
		Header: header,
	}
	log.Tracef("Sending KeepAliveResponse %+v", response)
	return response, nil
}

// CloseSession closes a session
func (s *Server) CloseSession(ctx context.Context, request *api.CloseSessionRequest) (*api.CloseSessionResponse, error) {
	log.Tracef("Received CloseSessionRequest %+v", request)
	header, err := s.DoCloseSession(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	response := &api.CloseSessionResponse{
		Header: header,
	}
	log.Tracef("Sending CloseSessionResponse %+v", response)
	return response, nil
}
