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
	storageapi "github.com/atomix/api/go/atomix/storage"
	api "github.com/atomix/api/go/atomix/storage/lock"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var log = logging.GetLogger("atomix", "lock")

// RegisterPrimitive registers the election primitive on the given node
func RegisterServer(node *proxy.Node) {
	node.RegisterServer(Type, &ServerType{})
}

// ServerType is the election primitive server
type ServerType struct{}

// RegisterServer registers the election server with the protocol
func (p *ServerType) RegisterServer(server *grpc.Server, client *proxy.Client) {
	api.RegisterLockServiceServer(server, &Server{
		Proxy: proxy.NewProxy(client),
	})
}

var _ proxy.PrimitiveServer = &ServerType{}

// Server is an implementation of MapServiceServer for the map primitive
type Server struct {
	*proxy.Proxy
}

// Create opens a new session
func (s *Server) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	log.Debugf("Received CreateRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	err := partition.DoCreateService(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	response := &api.CreateResponse{}
	log.Debugf("Sending CreateResponse %+v", response)
	return response, nil
}

// Close closes a session
func (s *Server) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
	log.Debugf("Received CloseRequest %+v", request)
	if request.Delete {
		partition := s.PartitionFor(request.Header.Primitive)
		err := partition.DoDeleteService(ctx, request.Header)
		if err != nil {
			return nil, err
		}
		response := &api.CloseResponse{}
		log.Debugf("Sending CloseResponse %+v", response)
		return response, nil
	}

	partition := s.PartitionFor(request.Header.Primitive)
	err := partition.DoCloseService(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	response := &api.CloseResponse{}
	log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}

// Lock acquires a lock
func (s *Server) Lock(ctx context.Context, request *api.LockRequest) (*api.LockResponse, error) {
	log.Debugf("Received LockRequest %+v", request)

	in, err := proto.Marshal(&LockRequest{
		Timeout: request.Timeout,
	})
	if err != nil {
		return nil, err
	}

	stream := streams.NewBufferedStream()
	partition := s.PartitionFor(request.Header.Primitive)
	if err := partition.DoCommandStream(ctx, opLock, in, request.Header, stream); err != nil {
		return nil, err
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			return nil, status.Error(codes.Canceled, "stream closed")
		}

		if result.Failed() {
			return nil, result.Error
		}

		output := result.Value.(proxy.SessionOutput)

		if output.Type == storageapi.ResponseType_RESPONSE {
			lockResponse := &LockResponse{}
			if err = proto.Unmarshal(output.Value.([]byte), lockResponse); err != nil {
				return nil, err
			}
			response := &api.LockResponse{
				Version: uint64(lockResponse.Index),
			}
			log.Debugf("Sending LockResponse %+v", response)
			return response, nil
		}
	}
}

// Unlock releases the lock
func (s *Server) Unlock(ctx context.Context, request *api.UnlockRequest) (*api.UnlockResponse, error) {
	log.Debugf("Received UnlockRequest %+v", request)
	in, err := proto.Marshal(&UnlockRequest{
		Index: int64(request.Version),
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opUnlock, in, request.Header)
	if err != nil {
		return nil, err
	}

	unlockResponse := &UnlockResponse{}
	if err = proto.Unmarshal(out, unlockResponse); err != nil {
		return nil, err
	}

	response := &api.UnlockResponse{
		Unlocked: unlockResponse.Succeeded,
	}
	log.Debugf("Sending UnlockResponse %+v", response)
	return response, nil
}

// IsLocked checks whether the lock is held by any session
func (s *Server) IsLocked(ctx context.Context, request *api.IsLockedRequest) (*api.IsLockedResponse, error) {
	log.Debugf("Received IsLockedRequest %+v", request)
	in, err := proto.Marshal(&IsLockedRequest{
		Index: int64(request.Version),
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoQuery(ctx, opIsLocked, in, request.Header)
	if err != nil {
		return nil, err
	}

	isLockedResponse := &IsLockedResponse{}
	if err = proto.Unmarshal(out, isLockedResponse); err != nil {
		return nil, err
	}

	response := &api.IsLockedResponse{
		IsLocked: isLockedResponse.Locked,
	}
	log.Debugf("Sending IsLockedResponse %+v", response)
	return response, nil
}
