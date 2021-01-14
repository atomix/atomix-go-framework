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
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/protocol/p2p"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc"
	"sync"
	"time"
)

var log = logging.GetLogger("atomix", "protocol", "p2p", "counter")

const gossipInterval = 100 * time.Millisecond

func init() {
	registerServerFunc = func(server *grpc.Server, manager *p2p.Manager) {
		RegisterCounterProtocolServer(server, newProtocolServer(newManager(manager)))
	}
	newServiceFunc = func(name string, partition *cluster.Partition) p2p.Service {
		return newService(name, partition)
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

func newService(name string, partition *cluster.Partition) Service {
	return &counterService{
		name:      name,
		partition: partition,
		peers:     make(map[cluster.ReplicaID]*counterPeer),
		gossipCh:  make(chan meta.LogicalTimestamp),
	}
}

type counterService struct {
	name        string
	partition   *cluster.Partition
	state       CounterState
	stateMu     sync.RWMutex
	timestamp   meta.LogicalTimestamp
	peers       map[cluster.ReplicaID]*counterPeer
	peersMu     sync.RWMutex
	gossipCh    chan meta.LogicalTimestamp
	gossipTimer *time.Timer
	gossipMu    sync.RWMutex
	cancel      context.CancelFunc
}

func (s *counterService) start() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	watchCh := make(chan cluster.ReplicaSet)
	err := s.partition.Watch(ctx, watchCh)
	if err != nil {
		return err
	}

	replicaSet := s.partition.Replicas()
	err = s.updateReplicas(replicaSet)
	if err != nil {
		return err
	}

	go s.watchReplicas(watchCh)
	go s.sendStates()
	return nil
}

func (s *counterService) watchReplicas(watchCh <-chan cluster.ReplicaSet) {
	for replicaSet := range watchCh {
		err := s.updateReplicas(replicaSet)
		if err != nil {
			log.Errorf("Failed to handle replica set change: %v", err)
		}
	}
}

func (s *counterService) updateReplicas(replicaSet cluster.ReplicaSet) error {
	replicas := make([]*cluster.Replica, 0, len(replicaSet))
	for _, replica := range replicaSet {
		if member, ok := s.partition.Member(); !ok || replica.ID != member.ID {
			replicas = append(replicas, replica)
		}
	}

	err := async.IterAsync(len(replicas), func(i int) error {
		replica := replicas[i]
		s.peersMu.RLock()
		_, ok := s.peers[replica.ID]
		s.peersMu.RUnlock()
		if ok {
			return nil
		}

		s.peersMu.Lock()
		defer s.peersMu.Unlock()

		peer, err := newPeer(s, replica)
		if err != nil {
			return err
		}
		s.peers[peer.replica.ID] = peer
		return nil
	})
	if err != nil {
		return err
	}

	s.peersMu.RLock()
	peers := make([]*counterPeer, 0, len(s.peers))
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}
	s.peersMu.RUnlock()

	err = async.IterAsync(len(peers), func(i int) error {
		peer := peers[i]
		if _, ok := replicaSet[peer.replica.ID]; ok {
			return nil
		}

		s.peersMu.Lock()
		defer s.peersMu.Unlock()

		peer.close()
		delete(s.peers, peer.replica.ID)
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *counterService) sendStates() {
	for range s.gossipCh {

	}
}

func (s *counterService) enqueueState() {
	s.gossipMu.RLock()
	gossipTimer := s.gossipTimer
	s.gossipMu.RUnlock()
	if gossipTimer != nil {
		return
	}

	s.gossipMu.Lock()
	defer s.gossipMu.Unlock()

	gossipTimer = s.gossipTimer
	if gossipTimer != nil {
		return
	}

	gossipTimer = time.NewTimer(gossipInterval)
	s.gossipTimer = gossipTimer

	go func() {
		<-gossipTimer.C
		s.gossipMu.Lock()
		s.gossipTimer = nil
		s.gossipMu.Unlock()

		s.stateMu.RLock()
		timestamp := s.timestamp
		s.stateMu.RUnlock()
		s.gossipCh <- timestamp
	}()
}

func (s *counterService) Set(ctx context.Context, input *counter.SetInput) (*counter.SetOutput, error) {
	return nil, errors.NewNotSupported("Set is not supported for CRDT counters")
}

func (s *counterService) getValue() int64 {
	var value int64
	for _, inc := range s.state.Increments {
		value += int64(inc)
	}
	for _, dec := range s.state.Decrements {
		value -= int64(dec)
	}
	return value
}

func (s *counterService) Get(ctx context.Context, input *counter.GetInput) (*counter.GetOutput, error) {
	s.stateMu.RLock()
	value := s.getValue()
	s.stateMu.RUnlock()
	return &counter.GetOutput{
		Value: value,
	}, nil
}

func (s *counterService) Increment(ctx context.Context, input *counter.IncrementInput) (*counter.IncrementOutput, error) {
	member, ok := s.partition.Member()
	if !ok {
		return nil, errors.NewUnavailable("not a member of the partition")
	}
	s.stateMu.Lock()
	prevValue := s.getValue()
	increment := s.state.Increments[string(member.ID)]
	s.state.Increments[string(member.ID)] = increment + 1
	s.timestamp.Increment()
	nextValue := s.getValue()
	s.stateMu.Unlock()
	s.enqueueState()
	return &counter.IncrementOutput{
		PreviousValue: prevValue,
		NextValue:     nextValue,
	}, nil
}

func (s *counterService) Decrement(ctx context.Context, input *counter.DecrementInput) (*counter.DecrementOutput, error) {
	member, ok := s.partition.Member()
	if !ok {
		return nil, errors.NewUnavailable("not a member of the partition")
	}
	s.stateMu.Lock()
	prevValue := s.getValue()
	decrement := s.state.Decrements[string(member.ID)]
	s.state.Decrements[string(member.ID)] = decrement + 1
	s.timestamp.Increment()
	nextValue := s.getValue()
	s.stateMu.Unlock()
	s.enqueueState()
	return &counter.DecrementOutput{
		PreviousValue: prevValue,
		NextValue:     nextValue,
	}, nil
}

func (s *counterService) CheckAndSet(ctx context.Context, input *counter.CheckAndSetInput) (*counter.CheckAndSetOutput, error) {
	return nil, errors.NewNotSupported("CheckAndSet is not supported for CRDT counters")
}

func (s *counterService) Snapshot(ctx context.Context) (*counter.Snapshot, error) {
	return nil, errors.NewNotSupported("Snapshot is not supported for CRDT counters")
}

func (s *counterService) Restore(ctx context.Context, input *counter.Snapshot) error {
	return errors.NewNotSupported("Restore is not supported for CRDT counters")
}

func (s *counterService) Update(ctx context.Context, input CounterState) (CounterState, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	for id, inc := range input.Increments {
		if inc > s.state.Increments[id] {
			s.state.Increments[id] = inc
		}
	}
	for id, dec := range input.Decrements {
		if dec > s.state.Decrements[id] {
			s.state.Decrements[id] = dec
		}
	}
	return s.state, nil
}

func (s *counterService) close() {
	s.cancel()
}

func newPeer(service *counterService, replica *cluster.Replica) (*counterPeer, error) {
	peer := &counterPeer{
		service: service,
		replica: replica,
		ticker:  time.NewTicker(gossipInterval),
	}
	err := peer.connect()
	if err != nil {
		return nil, err
	}
	return peer, nil
}

type counterPeer struct {
	service *counterService
	replica *cluster.Replica
	conn    *grpc.ClientConn
	client  CounterProtocolClient
	ticker  *time.Ticker
}

func (p *counterPeer) connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := p.replica.Connect(ctx, cluster.WithDialOption(grpc.WithInsecure()))
	if err != nil {
		return err
	}
	p.conn = conn
	p.client = NewCounterProtocolClient(conn)
	go p.gossip()
	return nil
}

func (p *counterPeer) gossip() {
	for range p.ticker.C {

	}
}

func (p *counterPeer) close() {

}
