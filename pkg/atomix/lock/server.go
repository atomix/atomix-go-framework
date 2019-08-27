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

package lock

import (
	"context"
	"github.com/atomix/atomix-api/proto/atomix/headers"
	api "github.com/atomix/atomix-api/proto/atomix/lock"
	"github.com/atomix/atomix-go-node/pkg/atomix/server"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// RegisterServer registers a lock server with the given gRPC server
func RegisterServer(server *grpc.Server, client service.Client) {
	api.RegisterLockServiceServer(server, newServer(client))
}

func newServer(client service.Client) api.LockServiceServer {
	return &Server{
		SessionizedServer: &server.SessionizedServer{
			Type:   "lock",
			Client: client,
		},
	}
}

// Server is an implementation of MapServiceServer for the map primitive
type Server struct {
	*server.SessionizedServer
}

// Create opens a new session
func (s *Server) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	log.Tracef("Received CreateRequest %+v", request)
	session, err := s.OpenSession(ctx, request.Header, request.Timeout)
	if err != nil {
		return nil, err
	}
	response := &api.CreateResponse{
		Header: &headers.ResponseHeader{
			SessionID: session,
			Index:     session,
		},
	}
	log.Tracef("Sending CreateResponse %+v", response)
	return response, nil
}

// KeepAlive keeps an existing session alive
func (s *Server) KeepAlive(ctx context.Context, request *api.KeepAliveRequest) (*api.KeepAliveResponse, error) {
	log.Tracef("Received KeepAliveRequest %+v", request)
	if err := s.KeepAliveSession(ctx, request.Header); err != nil {
		return nil, err
	}
	response := &api.KeepAliveResponse{
		Header: &headers.ResponseHeader{
			SessionID: request.Header.SessionID,
		},
	}
	log.Tracef("Sending KeepAliveResponse %+v", response)
	return response, nil
}

// Close closes a session
func (s *Server) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
	log.Tracef("Received CloseRequest %+v", request)
	if request.Delete {
		if err := s.Delete(ctx, request.Header); err != nil {
			return nil, err
		}
	} else {
		if err := s.CloseSession(ctx, request.Header); err != nil {
			return nil, err
		}
	}

	response := &api.CloseResponse{
		Header: &headers.ResponseHeader{
			SessionID: request.Header.SessionID,
		},
	}
	log.Tracef("Sending CloseResponse %+v", response)
	return response, nil
}

// Lock acquires a lock
func (s *Server) Lock(ctx context.Context, request *api.LockRequest) (*api.LockResponse, error) {
	log.Tracef("Received LockRequest %+v", request)

	in, err := proto.Marshal(&LockRequest{
		Timeout: request.Timeout,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.Command(ctx, "lock", in, request.Header)
	if err != nil {
		return nil, err
	}

	lockResponse := &LockResponse{}
	if err = proto.Unmarshal(out, lockResponse); err != nil {
		return nil, err
	}

	response := &api.LockResponse{
		Header:  header,
		Version: uint64(lockResponse.Index),
	}
	log.Tracef("Sending LockResponse %+v", response)
	return response, nil
}

// Unlock releases the lock
func (s *Server) Unlock(ctx context.Context, request *api.UnlockRequest) (*api.UnlockResponse, error) {
	log.Tracef("Received UnlockRequest %+v", request)
	in, err := proto.Marshal(&UnlockRequest{
		Index: int64(request.Version),
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.Command(ctx, "unlock", in, request.Header)
	if err != nil {
		return nil, err
	}

	unlockResponse := &UnlockResponse{}
	if err = proto.Unmarshal(out, unlockResponse); err != nil {
		return nil, err
	}

	response := &api.UnlockResponse{
		Header:   header,
		Unlocked: unlockResponse.Succeeded,
	}
	log.Tracef("Sending UnlockResponse %+v", response)
	return response, nil
}

// IsLocked checks whether the lock is held by any session
func (s *Server) IsLocked(ctx context.Context, request *api.IsLockedRequest) (*api.IsLockedResponse, error) {
	log.Tracef("Received IsLockedRequest %+v", request)
	in, err := proto.Marshal(&IsLockedRequest{
		Index: int64(request.Version),
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.Query(ctx, "islocked", in, request.Header)
	if err != nil {
		return nil, err
	}

	isLockedResponse := &IsLockedResponse{}
	if err = proto.Unmarshal(out, isLockedResponse); err != nil {
		return nil, err
	}

	response := &api.IsLockedResponse{
		Header:   header,
		IsLocked: isLockedResponse.Locked,
	}
	log.Tracef("Sending IsLockedResponse %+v", response)
	return response, nil
}
