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
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
)

// Server is a base server for servers that support sessions
type Server struct {
	Protocol Protocol
}

func (s *Server) Request(request *StorageRequest, stream StorageService_RequestServer) error {
	log.Debugf("Received StorageRequest %+v", request)

	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(PartitionID(request.PartitionID))
	if partition.MustLeader() && !partition.IsLeader() {
		response := &StorageResponse{
			PartitionID: request.PartitionID,
			Response: &SessionResponse{
				Type: SessionResponseType_RESPONSE,
				Status: SessionResponseStatus{
					Code:   SessionResponseCode_NOT_LEADER,
					Leader: partition.Leader(),
				},
			},
		}
		log.Debugf("Sending StorageResponse %+v", response)
		return stream.Send(response)
	}

	bytes, err := proto.Marshal(request.Request)
	if err != nil {
		log.Debugf("StorageRequest %+v failed: %s", request, err)
		return err
	}

	ch := make(chan streams.Result)
	inStream := streams.NewChannelStream(ch)

	switch request.Request.Request.(type) {
	case *SessionRequest_Command:
		go partition.ExecuteCommand(stream.Context(), bytes, inStream)
	case *SessionRequest_Query:
		go partition.ExecuteQuery(stream.Context(), bytes, inStream)
	case *SessionRequest_OpenSession:
		go partition.ExecuteCommand(stream.Context(), bytes, inStream)
	case *SessionRequest_KeepAlive:
		go partition.ExecuteCommand(stream.Context(), bytes, inStream)
	case *SessionRequest_CloseSession:
		go partition.ExecuteCommand(stream.Context(), bytes, inStream)
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
		log.Debugf("Sending StorageResponse %+v", response)
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return nil
}

var _ StorageServiceServer = &Server{}
