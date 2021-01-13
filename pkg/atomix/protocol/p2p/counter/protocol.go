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

package counter

import (
	"context"
	"github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/protocol/p2p"
	"google.golang.org/grpc"
)

func init() {
	registerServerFunc = func(server *grpc.Server, manager *p2p.Manager) {
		RegisterCounterProtocolServer(server, newProtocolServer(newManager(manager)))
	}
	newServiceFunc = func(partition *cluster.Partition) p2p.Service {
		return newService(partition)
	}
}

func newProtocolServer(manager *Manager) CounterProtocolServer {
	return &ProtocolServer{
		manager: manager,
	}
}

type ProtocolServer struct {
	manager *Manager
}

func (s *ProtocolServer) getService(header RequestHeader) (UpdateService, error) {
	partition, err := s.manager.Partition(header.PartitionID)
	if err != nil {
		return nil, err
	}
	service, err := partition.GetService(header.Service)
	if err != nil {
		return nil, err
	}
	return service.(UpdateService), nil
}

func (s *ProtocolServer) Update(ctx context.Context, request *UpdateRequest) (*UpdateResponse, error) {
	service, err := s.getService(request.Header)
	if err != nil {
		return nil, errors.Proto(err)
	}
	state, err := service.(UpdateService).Update(ctx, request.State)
	if err != nil {
		return nil, errors.Proto(err)
	}
	return &UpdateResponse{
		State: state,
	}, nil
}

type UpdateService interface {
	Update(context.Context, CounterState) (CounterState, error)
}

func newService(partition *cluster.Partition) Service {
	return &counterService{
		partition: partition,
	}
}

type counterService struct {
	partition *cluster.Partition
	state     CounterState
}

func (s *counterService) Set(ctx context.Context, input *counter.SetInput) (*counter.SetOutput, error) {
	panic("implement me")
}

func (s *counterService) Get(ctx context.Context, input *counter.GetInput) (*counter.GetOutput, error) {
	panic("implement me")
}

func (s *counterService) Increment(ctx context.Context, input *counter.IncrementInput) (*counter.IncrementOutput, error) {
	panic("implement me")
}

func (s *counterService) Decrement(ctx context.Context, input *counter.DecrementInput) (*counter.DecrementOutput, error) {
	panic("implement me")
}

func (s *counterService) CheckAndSet(ctx context.Context, input *counter.CheckAndSetInput) (*counter.CheckAndSetOutput, error) {
	panic("implement me")
}

func (s *counterService) Snapshot(ctx context.Context) (*counter.Snapshot, error) {
	panic("implement me")
}

func (s *counterService) Restore(ctx context.Context, input *counter.Snapshot) error {
	panic("implement me")
}

func (s *counterService) Update(ctx context.Context, input CounterState) (CounterState, error) {
	panic("implement me")
}
