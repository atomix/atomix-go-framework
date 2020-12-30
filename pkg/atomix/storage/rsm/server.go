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

package rsm

import (
	"context"
	"github.com/atomix/api/go/atomix/storage"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
)

// Server is a base server for servers that support sessions
type Server struct {
	Protocol Protocol
}

func (s *Server) Request(request *StorageRequest, stream StorageService_RequestServer) error {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(PartitionID(request.PartitionID))
	if partition.MustLeader() && !partition.IsLeader() {
		return stream.Send(&StorageResponse{
			PartitionID: request.PartitionID,
			Response: &SessionResponse{
				Type: SessionResponseType_RESPONSE,
				Status: SessionResponseStatus{
					Code: SessionResponseCode_NOT_LEADER,
				},
			},
		})
	}

	bytes, err := proto.Marshal(request.Request)
	if err != nil {
		return err
	}

	ch := make(chan streams.Result)
	inStream := streams.NewChannelStream(ch)

	switch request.Request.Request.(type) {
	case *SessionRequest_Command:
		err = partition.ExecuteCommand(stream.Context(), bytes, inStream)
	case *SessionRequest_Query:
		err = partition.ExecuteQuery(stream.Context(), bytes, inStream)
	case *SessionRequest_OpenSession:
		err = partition.ExecuteCommand(stream.Context(), bytes, inStream)
	case *SessionRequest_KeepAlive:
		err = partition.ExecuteCommand(stream.Context(), bytes, inStream)
	case *SessionRequest_CloseSession:
		err = partition.ExecuteCommand(stream.Context(), bytes, inStream)
	}

	for result := range ch {
		if result.Failed() {
			return result.Error
		}

		sessionResponse := &SessionResponse{}
		if err := proto.Unmarshal(result.Value.([]byte), sessionResponse); err != nil {
			return err
		}

		response := &StorageResponse{
			PartitionID: request.PartitionID,
			Response:    sessionResponse,
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return nil
}

var _ StorageServiceServer = &Server{}

// ConfigServer is a server for updating the storage configuration
type ConfigServer struct {
	cluster *cluster.Cluster
}

func (s *ConfigServer) Update(ctx context.Context, request *storage.UpdateRequest) (*storage.UpdateResponse, error) {
	if err := s.cluster.Update(request.Config); err != nil {
		return nil, err
	}
	return &storage.UpdateResponse{}, nil
}

var _ storage.StorageServiceServer = &ConfigServer{}
