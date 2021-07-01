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
	"io"
	"time"
)

// Server is a base server for servers that support sessions
type Server struct {
	Protocol Protocol
}

func (s *Server) Read(ctx context.Context, request *PartitionReadRequest) (*PartitionReadResponse, error) {
	log.Debugf("Received StorageRequest %+v", request)

	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(PartitionID(request.PartitionID))
	if partition.MustLeader() && !partition.IsLeader() {
		response := &PartitionReadResponse{
			Status: PartitionResponseStatus{
				Code:   PartitionResponseCode_PARTITION_NOT_LEADER,
				Leader: partition.Leader(),
			},
		}
		log.Debugf("Sending PartitionReadResponse %+v", response)
		return response, nil
	}

	smRequest := &StateMachineReadRequest{
		Request: request.Request,
	}

	bytes, err := proto.Marshal(smRequest)
	if err != nil {
		log.Debugf("StorageRequest %+v failed: %s", request, err)
		return nil, err
	}

	resultCh := make(chan streams.Result, 1)
	partition.Read(bytes, streams.NewChannelStream(resultCh))

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

		smResponse := &StateMachineReadResponse{}
		if err := proto.Unmarshal(result.Value.([]byte), smResponse); err != nil {
			return nil, err
		}

		response := &PartitionReadResponse{
			Response: smResponse.Response,
		}
		log.Debugf("Sending StorageResponse %+v", response)
		return response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Server) ReadStream(request *PartitionReadRequest, srv PartitionService_ReadStreamServer) error {
	log.Debugf("Received PartitionReadRequest %+v", request)

	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(PartitionID(request.PartitionID))
	if partition.MustLeader() && !partition.IsLeader() {
		response := &PartitionReadResponse{
			Status: PartitionResponseStatus{
				Code:   PartitionResponseCode_PARTITION_NOT_LEADER,
				Leader: partition.Leader(),
			},
		}
		log.Debugf("Sending PartitionReadResponse %+v", response)
		return srv.Send(response)
	}

	smRequest := &StateMachineReadRequest{
		Request: request.Request,
	}

	bytes, err := proto.Marshal(smRequest)
	if err != nil {
		log.Debugf("PartitionReadRequest %+v failed: %s", request, err)
		return err
	}

	resultCh := make(chan streams.Result)
	stream := streams.NewChannelStream(resultCh)
	defer stream.Drain()

	partition.Read(bytes, stream)

	for {
		select {
		case result, ok := <-resultCh:
			if !ok {
				log.Debugf("Finished PartitionReadResponse %+v", request)
				return nil
			}

			if result.Failed() {
				log.Warnf("PartitionReadRequest %+v failed: %v", request, result.Error)
				return result.Error
			}

			smResponse := &StateMachineReadResponse{}
			if err := proto.Unmarshal(result.Value.([]byte), smResponse); err != nil {
				return err
			}

			response := &PartitionReadResponse{
				Response: smResponse.Response,
			}
			log.Debugf("Sending PartitionReadResponse %+v", response)
			if err := srv.Send(response); err != nil {
				return err
			}
		case <-srv.Context().Done():
			err := srv.Context().Err()
			log.Debugf("Finished PartitionReadRequest %+v: %v", request, err)
			return err
		}
	}
}

func (s *Server) WriteStream(srv PartitionService_WriteStreamServer) error {
	resultCh := make(chan streams.Result)
	errCh := make(chan error)
	stream := streams.NewChannelStream(resultCh)
	defer stream.Drain()
	go func() {
		defer close(errCh)
		var partition Partition
		for {
			request, err := srv.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				errCh <- err
				return
			}

			log.Debugf("Received PartitionWriteRequest %+v", request)
			if partition == nil {
				partition = s.Protocol.Partition(PartitionID(request.PartitionID))
			}

			if partition.MustLeader() && !partition.IsLeader() {
				response := &PartitionWriteResponse{
					Status: PartitionResponseStatus{
						Code:   PartitionResponseCode_PARTITION_NOT_LEADER,
						Leader: partition.Leader(),
					},
				}
				resultCh <- streams.Result{
					Value: response,
				}
				continue
			}

			smRequest := &StateMachineWriteRequest{
				Timestamp: time.Now(),
				Request:   request.Request,
			}

			bytes, err := proto.Marshal(smRequest)
			if err != nil {
				log.Debugf("PartitionWriteRequest %+v failed: %s", request, err)
				continue
			}
			partition.Write(bytes, stream)
		}
	}()

	for {
		select {
		case result, ok := <-resultCh:
			if !ok {
				return nil
			}

			if result.Failed() {
				log.Warnf("PartitionWriteRequest failed: %v", result.Error)
				return result.Error
			}

			smResponse := &StateMachineWriteResponse{}
			if err := proto.Unmarshal(result.Value.([]byte), smResponse); err != nil {
				return err
			}

			response := &PartitionWriteResponse{
				Response: smResponse.Response,
			}
			log.Debugf("Sending PartitionWriteResponse %+v", response)
			if err := srv.Send(response); err != nil {
				return err
			}
		case err := <-errCh:
			log.Debugf("Write stream failed: %v", err)
			return err
		case <-srv.Context().Done():
			err := srv.Context().Err()
			log.Debugf("Finished Write stream: %v", err)
			return err
		}
	}
}

var _ PartitionServiceServer = &Server{}
