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

package value

import (
	"context"
	metaapi "github.com/atomix/api/go/atomix/primitive/meta"
	valueapi "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/protocol/p2p"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc"
	"io"
	"sync"
	"time"
)

var log = logging.GetLogger("atomix", "protocol", "p2p", "value")

const antiEntropyPeriod = time.Second

func init() {
	registerServerFunc = func(server *grpc.Server, manager *p2p.Manager) {
		RegisterValueProtocolServer(server, newProtocolServer(newManager(manager)))
	}
	newServiceFunc = func(name string, partition *cluster.Partition) p2p.Service {
		return newService(name, partition)
	}
}

func newProtocolServer(manager *Manager) ValueProtocolServer {
	return &ProtocolServer{
		manager: manager,
	}
}

type ProtocolServer struct {
	manager *Manager
}

func (s *ProtocolServer) Gossip(stream ValueProtocol_GossipServer) error {
	msg, err := stream.Recv()
	if err == io.EOF {
		return nil
	} else if err != nil {
		log.Errorf("Receiving GossipMessage %+v failed: %v", msg, err)
		return err
	}

	log.Debugf("Received GossipMessage %+v", msg)
	bootstrap := msg.GetBootstrap()
	if bootstrap == nil {
		return errors.Proto(errors.NewInvalid("gossip stream not initialized with expected bootstrap request"))
	}

	service, err := s.getService(bootstrap.PartitionID, bootstrap.Service)
	if err != nil {
		return errors.Proto(err)
	}

	value, err := service.Bootstrap(stream.Context())
	if err != nil {
		return errors.Proto(err)
	}

	msg = &GossipMessage{
		Message: &GossipMessage_Update{
			Update: &Update{
				Value: value,
			},
		},
	}
	log.Debugf("Sending GossipMessage %+v", msg)
	err = stream.Send(msg)
	if err != nil {
		log.Errorf("Sending GossipMessage %+v failed: %v", msg, err)
		return err
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			log.Errorf("Receiving GossipMessage %+v failed: %v", msg, err)
			return err
		}

		log.Debugf("Received GossipMessage %+v", msg)
	}
}

func (s *ProtocolServer) getService(partitionID p2p.PartitionID, name string) (GossipService, error) {
	partition, err := s.manager.Partition(partitionID)
	if err != nil {
		return nil, err
	}
	service, err := partition.GetService(name)
	if err != nil {
		return nil, err
	}
	return service.(GossipService), nil
}

type GossipService interface {
	Bootstrap(context.Context) (Value, error)
	Advertise(context.Context, Digest) (request bool, err error)
	Update(context.Context, Update) (update *Update, err error)
}

func newService(name string, partition *cluster.Partition) Service {
	service := &valueService{
		name:      name,
		partition: partition,
		peers:     make(map[cluster.ReplicaID]*valuePeer),
	}
	service.start()
	return service
}

type valueService struct {
	name      string
	partition *cluster.Partition
	value     valueapi.Value
	valueMu   sync.RWMutex
	streams   []ServiceEventsStream
	peers     map[cluster.ReplicaID]*valuePeer
	peersMu   sync.RWMutex
	cancel    context.CancelFunc
}

func (s *valueService) start() error {
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
	go s.sendAdvertisements()
	return nil
}

func (s *valueService) watchReplicas(watchCh <-chan cluster.ReplicaSet) {
	for replicaSet := range watchCh {
		err := s.updateReplicas(replicaSet)
		if err != nil {
			log.Errorf("Failed to handle replica set change: %v", err)
		}
	}
}

func (s *valueService) updateReplicas(replicaSet cluster.ReplicaSet) error {
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
	peers := make([]*valuePeer, 0, len(s.peers))
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

func (s *valueService) sendAdvertisements() {
	ticker := time.NewTicker(antiEntropyPeriod)
	for range ticker.C {
		s.enqueueAdvertisements()
	}
}

func (s *valueService) enqueueAdvertisements() {
	s.valueMu.RLock()
	digest := Digest{}
	if s.value.Timestamp != nil {
		digest.Timestamp = *s.value.Timestamp
	}
	s.valueMu.RUnlock()

	s.peersMu.RLock()
	for _, replica := range s.peers {
		go func(replica *valuePeer) {
			replica.enqueueAdvertisement(Advertisement{
				Digest: digest,
			})
		}(replica)
	}
	s.peersMu.RUnlock()
}

func (s *valueService) enqueueUpdate(update Update) {
	s.peersMu.RLock()
	for _, replica := range s.peers {
		go func(replica *valuePeer) {
			replica.enqueueUpdate(update)
		}(replica)
	}
	s.peersMu.RUnlock()
}

func (s *valueService) Set(ctx context.Context, input *valueapi.SetInput) (*valueapi.SetOutput, error) {
	value := input.Value
	if err := checkPreconditions(value, input.Preconditions); err != nil {
		return nil, err
	}

	s.valueMu.Lock()
	s.value = value
	s.valueMu.Unlock()
	return &valueapi.SetOutput{
		Value: value,
	}, nil
}

func (s *valueService) Get(ctx context.Context, input *valueapi.GetInput) (*valueapi.GetOutput, error) {
	s.valueMu.RLock()
	value := s.value
	s.valueMu.RUnlock()
	return &valueapi.GetOutput{
		Value: value,
	}, nil
}

func (s *valueService) Events(ctx context.Context, input *valueapi.EventsInput, stream ServiceEventsStream) error {
	s.valueMu.Lock()
	defer s.valueMu.Unlock()
	s.streams = append(s.streams, stream)
	return nil
}

func (s *valueService) Snapshot(ctx context.Context) (*valueapi.Snapshot, error) {
	return &valueapi.Snapshot{
		Value: &s.value,
	}, nil
}

func (s *valueService) Restore(ctx context.Context, snapshot *valueapi.Snapshot) error {
	s.value = *snapshot.Value
	return nil
}

func (s *valueService) Bootstrap(ctx context.Context) (Value, error) {
	s.valueMu.RLock()
	defer s.valueMu.RUnlock()
	var timestamp metaapi.Timestamp
	if s.value.Timestamp != nil {
		timestamp = *s.value.Timestamp
	}
	value := Value{
		Value: s.value.Value,
		Digest: Digest{
			Timestamp: timestamp,
		},
	}
	return value, nil
}

func (s *valueService) Advertise(ctx context.Context, digest Digest) (bool, error) {
	s.valueMu.RLock()
	value := s.value
	s.valueMu.RUnlock()
	localTimestamp := meta.NewTimestamp(*value.Timestamp)
	updateTimestamp := meta.NewTimestamp(digest.Timestamp)
	if updateTimestamp.After(localTimestamp) {
		return true, nil
	}
	return false, nil
}

func (s *valueService) Update(ctx context.Context, update Update) (*Update, error) {
	remoteValue := update.Value
	s.valueMu.RLock()
	localValue := s.value
	s.valueMu.RUnlock()

	localTimestamp := meta.NewTimestamp(*localValue.Timestamp)
	remoteTimestamp := meta.NewTimestamp(remoteValue.Digest.Timestamp)
	if remoteTimestamp.After(localTimestamp) {
		s.valueMu.Lock()
		defer s.valueMu.Unlock()
		localValue := s.value
		localTimestamp := meta.NewTimestamp(*localValue.Timestamp)
		remoteTimestamp := meta.NewTimestamp(remoteValue.Digest.Timestamp)
		if remoteTimestamp.After(localTimestamp) {
			timestamp := remoteTimestamp.Proto()
			s.value = valueapi.Value{
				ObjectMeta: metaapi.ObjectMeta{
					Timestamp: &timestamp,
				},
			}
		} else if localTimestamp.Before(remoteTimestamp) {
			return &Update{
				Value: Value{
					Value: localValue.Value,
					Digest: Digest{
						Timestamp: *localValue.Timestamp,
					},
				},
			}, nil
		}
	} else if localTimestamp.Before(remoteTimestamp) {
		return &Update{
			Value: Value{
				Value: localValue.Value,
				Digest: Digest{
					Timestamp: *localValue.Timestamp,
				},
			},
		}, nil
	}
	return nil, nil
}

func (s *valueService) close() {
	s.cancel()
}

func newPeer(service *valueService, replica *cluster.Replica) (*valuePeer, error) {
	peer := &valuePeer{
		service: service,
		replica: replica,
	}
	err := peer.connect()
	if err != nil {
		return nil, err
	}
	return peer, nil
}

type valuePeer struct {
	service  *valueService
	replica  *cluster.Replica
	conn     *grpc.ClientConn
	stream   ValueProtocol_GossipClient
	advertCh chan Advertisement
	updateCh chan Update
}

func (r *valuePeer) connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := r.replica.Connect(ctx, cluster.WithDialOption(grpc.WithInsecure()))
	if err != nil {
		return err
	}
	r.conn = conn
	client := NewValueProtocolClient(conn)
	stream, err := client.Gossip(context.Background())
	if err != nil {
		return err
	}
	r.stream = stream
	err = r.bootstrap()
	if err != nil {
		return err
	}
	go r.gossip()
	return nil
}

func (r *valuePeer) bootstrap() error {
	msg := &GossipMessage{
		Message: &GossipMessage_Bootstrap{
			Bootstrap: &Bootstrap{
				PartitionID: p2p.PartitionID(r.service.partition.ID),
				Service:     r.service.name,
			},
		},
	}
	log.Debugf("Sending GossipMessage %+v", msg)
	err := r.stream.Send(msg)
	if err != nil {
		log.Errorf("Sending GossipMessage %+v failed: %v", msg, err)
		return err
	}
	return nil
}

func (r *valuePeer) enqueueAdvertisement(advertisement Advertisement) {
	r.advertCh <- advertisement
}

func (r *valuePeer) enqueueUpdate(update Update) {
	r.updateCh <- update
}

func (r *valuePeer) gossip() {
	for {
		select {
		case update := <-r.updateCh:
			r.sendUpdate(update)
		case advert := <-r.advertCh:
			r.sendAdvertisement(advert)
		}
	}
}

func (r *valuePeer) sendUpdate(update Update) {
	msg := &GossipMessage{
		Message: &GossipMessage_Update{
			Update: &update,
		},
	}
	log.Debugf("Sending GossipMessage %+v", msg)
	err := r.stream.Send(msg)
	if err != nil {
		log.Errorf("Sending GossipMessage %+v failed: %v", msg, err)
	}
}

func (r *valuePeer) sendAdvertisement(advert Advertisement) {
	msg := &GossipMessage{
		Message: &GossipMessage_Advertisement{
			Advertisement: &advert,
		},
	}
	log.Debugf("Sending GossipMessage %+v", msg)
	err := r.stream.Send(msg)
	if err != nil {
		log.Errorf("Sending GossipMessage %+v failed: %v", msg, err)
	}
}

func (r *valuePeer) close() {
	close(r.advertCh)
	close(r.updateCh)
}

func checkPreconditions(value valueapi.Value, preconditions []valueapi.Precondition) error {
	for _, precondition := range preconditions {
		switch p := precondition.Precondition.(type) {
		case *valueapi.Precondition_Metadata:
			if !meta.Equal(value.ObjectMeta, *p.Metadata) {
				return errors.NewConflict("metadata precondition failed")
			}
		}
	}
	return nil
}
