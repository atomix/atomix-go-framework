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
	"github.com/gogo/protobuf/proto"
	"time"
)

// Server is a base server for servers that support sessions
type Server struct {
	Protocol Protocol
}

func (s *Server) WatchConfig(request *PartitionConfigRequest, server PartitionService_WatchConfigServer) error {
	log.Debugf("Received PartitionConfigRequest %.250s", request)
	partition := s.Protocol.Partition(request.PartitionID)
	ch := make(chan PartitionConfig)
	if err := partition.WatchConfig(server.Context(), ch); err != nil {
		return err
	}
	for event := range ch {
		response := &PartitionConfigResponse{
			Leader:    event.Leader,
			Followers: event.Followers,
		}
		log.Debugf("Sending PartitionConfigResponse %.250s", response)
		if err := server.Send(response); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) GetConfig(ctx context.Context, request *PartitionConfigRequest) (*PartitionConfigResponse, error) {
	log.Debugf("Received PartitionConfigRequest %.250s", request)
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(request.PartitionID)
	response := &PartitionConfigResponse{
		Leader:    partition.Leader(),
		Followers: partition.Followers(),
	}
	log.Debugf("Sending PartitionConfigResponse %.250s", response)
	return response, nil
}

func (s *Server) Query(ctx context.Context, request *PartitionQueryRequest) (*PartitionQueryResponse, error) {
	log.Debugf("Received PartitionQueryRequest %.250s", request)

	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(request.PartitionID)

	bytes, err := proto.Marshal(&request.Request)
	if err != nil {
		err = errors.NewInvalid("could not marshal request", err)
		log.Debugf("PartitionQueryRequest %.250s failed: %s", request, err)
		return nil, errors.Proto(err)
	}

	resultCh := make(chan streams.Result, 1)
	errCh := make(chan error, 1)
	go func() {
		if request.Sync {
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

	select {
	case result, ok := <-resultCh:
		if !ok {
			err = errors.NewCanceled("stream closed")
			log.Debugf("PartitionQueryRequest %.250s failed: %s", request, err)
			return nil, errors.Proto(err)
		}

		if result.Failed() {
			log.Warnf("PartitionQueryRequest %.250s failed: %v", request, result.Error)
			return nil, errors.Proto(result.Error)
		}

		response := &PartitionQueryResponse{}
		if err := proto.Unmarshal(result.Value.([]byte), &response.Response); err != nil {
			err = errors.NewInvalid("could not unmarshal response", err)
			log.Warnf("PartitionCommandResponse %.250s failed: %v", request, err)
			return nil, errors.Proto(err)
		}
		log.Debugf("Sending PartitionQueryResponse %.250s", response)
		return response, nil
	case err := <-errCh:
		log.Debugf("PartitionQueryRequest %.250s failed: %s", request, err)
		return nil, errors.Proto(err)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Server) QueryStream(request *PartitionQueryRequest, srv PartitionService_QueryStreamServer) error {
	log.Debugf("Received PartitionQueryRequest %.250s", request)

	partition := s.Protocol.Partition(request.PartitionID)

	bytes, err := proto.Marshal(&request.Request)
	if err != nil {
		err = errors.NewInvalid("could not marshal request", err)
		log.Debugf("PartitionQueryRequest %.250s failed: %s", request, err)
		return errors.Proto(err)
	}

	resultCh := make(chan streams.Result)
	errCh := make(chan error)
	stream := streams.NewBufferedStream()
	go func() {
		defer close(resultCh)
		for {
			result, ok := stream.Receive()
			if !ok {
				return
			}
			resultCh <- result
		}
	}()
	go func() {
		if request.Sync {
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

	for {
		select {
		case result, ok := <-resultCh:
			if !ok {
				log.Debugf("Finished PartitionQueryRequest %.250s", request)
				return nil
			}

			if result.Failed() {
				log.Warnf("PartitionQueryRequest %.250s failed: %v", request, result.Error)
				return errors.Proto(result.Error)
			}

			response := &PartitionQueryResponse{}
			if err := proto.Unmarshal(result.Value.([]byte), &response.Response); err != nil {
				err = errors.NewInvalid("could not unmarshal response", err)
				log.Warnf("PartitionCommandResponse %.250s failed: %v", request, err)
				return errors.Proto(err)
			}

			log.Debugf("Sending PartitionQueryResponse %.250s", response)
			if err := srv.Send(response); err != nil {
				log.Warnf("PartitionCommandResponse %.250s failed: %v", request, err)
				return err
			}
		case err := <-errCh:
			log.Warnf("PartitionQueryRequest %.250s failed: %v", request, err)
			return errors.Proto(err)
		case <-srv.Context().Done():
			err := srv.Context().Err()
			log.Debugf("Finished PartitionQueryRequest %.250s: %v", request, err)
			return err
		}
	}
}

func (s *Server) Command(ctx context.Context, request *PartitionCommandRequest) (*PartitionCommandResponse, error) {
	log.Debugf("Received PartitionCommandRequest %.250s", request)

	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(request.PartitionID)
	if partition.MustLeader() && !partition.IsLeader() {
		err := errors.NewUnavailable("not the leader")
		log.Debugf("PartitionCommandRequest %.250s failed: %s", request, err)
		return nil, errors.Proto(err)
	}

	if request.Request.Timestamp == nil {
		timestamp := time.Now()
		request.Request.Timestamp = &timestamp
	}

	bytes, err := proto.Marshal(&request.Request)
	if err != nil {
		err = errors.NewInvalid("could not marshal request", err)
		log.Debugf("PartitionCommandRequest %.250s failed: %s", request, err)
		return nil, errors.Proto(err)
	}

	resultCh := make(chan streams.Result, 1)
	errCh := make(chan error, 1)
	go func() {
		err := partition.SyncCommand(context.Background(), bytes, streams.NewChannelStream(resultCh))
		if err != nil {
			errCh <- err
		}
	}()

	select {
	case result, ok := <-resultCh:
		if !ok {
			err = errors.NewCanceled("stream closed")
			log.Debugf("PartitionCommandRequest %.250s failed: %s", request, err)
			return nil, errors.Proto(err)
		}

		if result.Failed() {
			log.Warnf("PartitionCommandRequest %.250s failed: %v", request, result.Error)
			return nil, errors.Proto(result.Error)
		}

		response := &PartitionCommandResponse{}
		if err := proto.Unmarshal(result.Value.([]byte), &response.Response); err != nil {
			err = errors.NewInvalid("could not unmarshal response", err)
			log.Warnf("PartitionCommandResponse %.250s failed: %v", request, err)
			return nil, errors.Proto(err)
		}
		log.Debugf("Sending PartitionCommandResponse %.250s", response)
		return response, nil
	case err := <-errCh:
		log.Debugf("PartitionCommandRequest %.250s failed: %s", request, err)
		return nil, errors.Proto(err)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Server) CommandStream(request *PartitionCommandRequest, srv PartitionService_CommandStreamServer) error {
	log.Debugf("Received PartitionCommandRequest %.250s", request)

	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(request.PartitionID)
	if partition.MustLeader() && !partition.IsLeader() {
		err := errors.NewUnavailable("not the leader")
		log.Debugf("PartitionCommandRequest %.250s failed: %s", request, err)
		return errors.Proto(err)
	}

	if request.Request.Timestamp == nil {
		timestamp := time.Now()
		request.Request.Timestamp = &timestamp
	}

	bytes, err := proto.Marshal(&request.Request)
	if err != nil {
		err = errors.NewInvalid("could not marshal request", err)
		log.Debugf("PartitionCommandRequest %.250s failed: %s", request, err)
		return errors.Proto(err)
	}

	resultCh := make(chan streams.Result)
	errCh := make(chan error)
	stream := streams.NewBufferedStream()
	go func() {
		defer close(resultCh)
		for {
			result, ok := stream.Receive()
			if !ok {
				return
			}
			resultCh <- result
		}
	}()
	go func() {
		err := partition.SyncCommand(srv.Context(), bytes, stream)
		if err != nil {
			errCh <- err
		}
	}()

	for {
		select {
		case result, ok := <-resultCh:
			if !ok {
				log.Debugf("Finished PartitionCommandRequest %.250s", request)
				return nil
			}

			if result.Failed() {
				log.Warnf("PartitionCommandRequest %.250s failed: %v", request, result.Error)
				return errors.Proto(result.Error)
			}

			response := &PartitionCommandResponse{}
			if err := proto.Unmarshal(result.Value.([]byte), &response.Response); err != nil {
				err = errors.NewInvalid("could not unmarshal response", err)
				log.Warnf("PartitionCommandResponse %.250s failed: %v", request, err)
				return errors.Proto(err)
			}
			log.Debugf("Sending PartitionCommandResponse %.250s", response)
			if err := srv.Send(response); err != nil {
				log.Warnf("PartitionCommandResponse %.250s failed: %v", request, err)
				return err
			}
		case err := <-errCh:
			log.Warnf("PartitionCommandRequest %.250s failed: %v", request, err)
			return errors.Proto(err)
		case <-srv.Context().Done():
			err := srv.Context().Err()
			log.Debugf("Finished PartitionCommandRequest %.250s: %v", request, err)
			return err
		}
	}
}

var _ PartitionServiceServer = &Server{}
