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
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
	"time"
)

// Server is a base server for servers that support sessions
type Server struct {
	Protocol Protocol
}

func (s *Server) Request(ctx context.Context, request *StorageRequest) (*StorageResponse, error) {
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
		return response, nil
	}

	smRequest := &StateMachineRequest{
		Timestamp: time.Now(),
		Request:   request.Request,
	}

	bytes, err := proto.Marshal(smRequest)
	if err != nil {
		log.Debugf("StorageRequest %+v failed: %s", request, err)
		return nil, err
	}

	resultCh := make(chan streams.Result, 1)
	errCh := make(chan error, 1)
	switch r := request.Request.Request.(type) {
	case *SessionRequest_Command,
		*SessionRequest_OpenSession,
		*SessionRequest_KeepAlive,
		*SessionRequest_CloseSession:
		go func() {
			err := partition.SyncCommand(context.Background(), bytes, streams.NewChannelStream(resultCh))
			if err != nil {
				errCh <- err
			}
		}()
	case *SessionRequest_Query:
		go func() {
			if r.Query.Context.Sync {
				err := partition.SyncQuery(context.Background(), bytes, streams.NewChannelStream(resultCh))
				if err != nil {
					errCh <- err
				}
			} else {
				err := partition.StaleQuery(context.Background(), bytes, streams.NewChannelStream(resultCh))
				if err != nil {
					errCh <- err
				}
			}
		}()
	}

	select {
	case result, ok := <-resultCh:
		if !ok {
			err = errors.NewCanceled("stream closed")
			log.Debugf("StorageRequest %+v failed: %s", request, err)
			return nil, err
		}

		if result.Failed() {
			log.Warnf("StorageRequest %+v failed: %v", request, result.Error)
			return nil, result.Error
		}

		smResponse := &StateMachineResponse{}
		if err := proto.Unmarshal(result.Value.([]byte), smResponse); err != nil {
			return nil, err
		}

		response := &StorageResponse{
			PartitionID: request.PartitionID,
			Response:    smResponse.Response,
		}
		log.Debugf("Sending StorageResponse %+v", response)
		return response, nil
	case err := <-errCh:
		log.Debugf("StorageRequest %+v failed: %s", request, err)
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Server) Stream(request *StorageRequest, srv StorageService_StreamServer) error {
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
		return srv.Send(response)
	}

	smRequest := &StateMachineRequest{
		Timestamp: time.Now(),
		Request:   request.Request,
	}

	bytes, err := proto.Marshal(smRequest)
	if err != nil {
		log.Debugf("StorageRequest %+v failed: %s", request, err)
		return err
	}

	resultCh := make(chan streams.Result)
	errCh := make(chan error)
	stream := streams.NewChannelStream(resultCh)
	defer stream.Drain()
	switch r := request.Request.Request.(type) {
	case *SessionRequest_Command:
		go func() {
			err := partition.SyncCommand(srv.Context(), bytes, stream)
			if err != nil {
				errCh <- err
			}
		}()
	case *SessionRequest_Query:
		go func() {
			if r.Query.Context.Sync {
				err := partition.SyncQuery(srv.Context(), bytes, stream)
				if err != nil {
					errCh <- err
				}
			} else {
				err := partition.StaleQuery(srv.Context(), bytes, stream)
				if err != nil {
					errCh <- err
				}
			}
		}()
	}

	for {
		select {
		case result, ok := <-resultCh:
			if !ok {
				log.Debugf("Finished StorageRequest %+v", request)
				return nil
			}

			if result.Failed() {
				log.Warnf("StorageRequest %+v failed: %v", request, result.Error)
				return result.Error
			}

			smResponse := &StateMachineResponse{}
			if err := proto.Unmarshal(result.Value.([]byte), smResponse); err != nil {
				return err
			}

			response := &StorageResponse{
				PartitionID: request.PartitionID,
				Response:    smResponse.Response,
			}
			log.Debugf("Sending StorageResponse %+v", response)
			if err := srv.Send(response); err != nil {
				return err
			}
		case err := <-errCh:
			log.Warnf("StorageRequest %+v failed: %v", request, err)
			return err
		case <-srv.Context().Done():
			err := srv.Context().Err()
			log.Debugf("Finished StorageRequest %+v: %v", request, err)
			return err
		}
	}
}

var _ StorageServiceServer = &Server{}
