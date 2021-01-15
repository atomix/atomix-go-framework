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

package _map

import (
	"context"
	mapapi "github.com/atomix/api/go/atomix/primitive/map"
	metaapi "github.com/atomix/api/go/atomix/primitive/meta"
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

var log = logging.GetLogger("atomix", "protocol", "p2p", "map")

const antiEntropyPeriod = time.Second

func init() {
	registerServerFunc = func(server *grpc.Server, manager *p2p.Manager) {
		RegisterMapProtocolServer(server, newProtocolServer(newManager(manager)))
	}
	newServiceFunc = func(name string, partition *cluster.Partition) p2p.Service {
		return newService(name, partition)
	}
}

func newProtocolServer(manager *Manager) MapProtocolServer {
	return &ProtocolServer{
		manager: manager,
	}
}

type ProtocolServer struct {
	manager *Manager
}

func (s *ProtocolServer) Gossip(stream MapProtocol_GossipServer) error {
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

	entries, err := service.Bootstrap(stream.Context())
	if err != nil {
		return errors.Proto(err)
	}

	for _, entry := range entries {
		msg := &GossipMessage{
			Message: &GossipMessage_Update{
				Update: &Update{
					Entry: entry,
				},
			},
		}
		log.Debugf("Sending GossipMessage %+v", msg)
		err := stream.Send(msg)
		if err != nil {
			log.Errorf("Sending GossipMessage %+v failed: %v", msg, err)
			return err
		}
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
		switch m := msg.Message.(type) {
		case *GossipMessage_Advertisement:
			request, err := service.Advertise(stream.Context(), m.Advertisement.Key, m.Advertisement.Digest)
			if err != nil {
				return err
			}

			if request {

			}
		case *GossipMessage_Update:
			update, err := service.Update(stream.Context(), *m.Update)
			if err != nil {
				return err
			}
			if update != nil {
				err := stream.Send(&GossipMessage{
					Message: &GossipMessage_Update{
						Update: update,
					},
				})
				if err != nil {
					return err
				}
			}
		}
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
	Bootstrap(context.Context) ([]Entry, error)
	Advertise(context.Context, string, Digest) (request bool, err error)
	Update(context.Context, Update) (update *Update, err error)
}

func newService(name string, partition *cluster.Partition) Service {
	service := &mapService{
		name:      name,
		partition: partition,
		entries:   make(map[string]Entry),
		peers:     make(map[cluster.ReplicaID]*mapPeer),
	}
	service.start()
	return service
}

type mapService struct {
	name      string
	partition *cluster.Partition
	entries   map[string]Entry
	entriesMu sync.RWMutex
	streams   []ServiceEventsStream
	peers     map[cluster.ReplicaID]*mapPeer
	peersMu   sync.RWMutex
	cancel    context.CancelFunc
}

func (s *mapService) start() error {
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

func (s *mapService) watchReplicas(watchCh <-chan cluster.ReplicaSet) {
	for replicaSet := range watchCh {
		err := s.updateReplicas(replicaSet)
		if err != nil {
			log.Errorf("Failed to handle replica set change: %v", err)
		}
	}
}

func (s *mapService) updateReplicas(replicaSet cluster.ReplicaSet) error {
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
	peers := make([]*mapPeer, 0, len(s.peers))
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

func (s *mapService) sendAdvertisements() {
	ticker := time.NewTicker(antiEntropyPeriod)
	for range ticker.C {
		s.enqueueAdvertisements()
	}
}

func (s *mapService) enqueueAdvertisements() {
	s.entriesMu.RLock()
	digests := make(map[string]Digest)
	for _, entry := range s.entries {
		digests[entry.Key] = entry.Digest
	}
	s.entriesMu.RUnlock()

	s.peersMu.RLock()
	for _, replica := range s.peers {
		go func(replica *mapPeer) {
			for key, digest := range digests {
				replica.enqueueAdvertisement(Advertisement{
					Key:    key,
					Digest: digest,
				})
			}
		}(replica)
	}
	s.peersMu.RUnlock()
}

func (s *mapService) enqueueUpdate(update Update) {
	s.peersMu.RLock()
	for _, replica := range s.peers {
		go func(replica *mapPeer) {
			replica.enqueueUpdate(update)
		}(replica)
	}
	s.peersMu.RUnlock()
}

func (s *mapService) Size(ctx context.Context) (*mapapi.SizeOutput, error) {
	s.entriesMu.RLock()
	defer s.entriesMu.RUnlock()
	return &mapapi.SizeOutput{
		Size_: uint32(len(s.entries)),
	}, nil
}

func (s *mapService) Put(ctx context.Context, input *mapapi.PutInput) (*mapapi.PutOutput, error) {
	s.entriesMu.Lock()
	defer s.entriesMu.Unlock()
	s.entries[input.Entry.Key] = Entry{
		Key:   input.Entry.Key,
		Value: input.Entry.Value,
		Digest: Digest{
			Timestamp: *input.Entry.Timestamp,
		},
	}
	return &mapapi.PutOutput{
		Entry: mapapi.Entry{
			ObjectMeta: metaapi.ObjectMeta{
				Timestamp: input.Entry.Timestamp,
			},
			Key:   input.Entry.Key,
			Value: input.Entry.Value,
		},
	}, nil
}

func (s *mapService) Get(ctx context.Context, input *mapapi.GetInput) (*mapapi.GetOutput, error) {
	s.entriesMu.RLock()
	defer s.entriesMu.RUnlock()
	entry, ok := s.entries[input.Key]
	if !ok {
		return nil, errors.NewNotFound("key '%s' not found", input.Key)
	}
	return &mapapi.GetOutput{
		Entry: mapapi.Entry{
			ObjectMeta: metaapi.ObjectMeta{
				Timestamp: &entry.Digest.Timestamp,
			},
			Key:   entry.Key,
			Value: entry.Value,
		},
	}, nil
}

func (s *mapService) Remove(ctx context.Context, input *mapapi.RemoveInput) (*mapapi.RemoveOutput, error) {
	s.entriesMu.Lock()
	defer s.entriesMu.Unlock()
	entry, ok := s.entries[input.Key]
	if !ok {
		return nil, errors.NewNotFound("key '%s' not found", input.Key)
	}
	delete(s.entries, input.Key)
	return &mapapi.RemoveOutput{
		Entry: mapapi.Entry{
			ObjectMeta: metaapi.ObjectMeta{
				Timestamp: &entry.Digest.Timestamp,
			},
			Key:   entry.Key,
			Value: entry.Value,
		},
	}, nil
}

func (s *mapService) Clear(ctx context.Context) error {
	s.entriesMu.Lock()
	defer s.entriesMu.Unlock()
	for key, entry := range s.entries {
		entry.Digest.Tombstone = true
		s.entries[key] = entry
	}
	return nil
}

func (s *mapService) Events(ctx context.Context, input *mapapi.EventsInput, stream ServiceEventsStream) error {
	s.entriesMu.Lock()
	defer s.entriesMu.Unlock()
	s.streams = append(s.streams, stream)
	return nil
}

func (s *mapService) Entries(ctx context.Context, input *mapapi.EntriesInput, stream ServiceEntriesStream) error {
	s.entriesMu.RLock()
	defer s.entriesMu.RUnlock()
	for _, entry := range s.entries {
		err := stream.Notify(&mapapi.EntriesOutput{
			Entry: mapapi.Entry{
				ObjectMeta: metaapi.ObjectMeta{
					Timestamp: &entry.Digest.Timestamp,
				},
				Key:   entry.Key,
				Value: entry.Value,
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *mapService) Snapshot(ctx context.Context, writer ServiceSnapshotWriter) error {
	writer.Close()
	return nil
}

func (s *mapService) Restore(ctx context.Context, entry *mapapi.SnapshotEntry) error {
	return nil
}

func (s *mapService) Bootstrap(ctx context.Context) ([]Entry, error) {
	s.entriesMu.RLock()
	defer s.entriesMu.RUnlock()
	entries := make([]Entry, 0, len(s.entries))
	for _, entry := range s.entries {
		entries = append(entries, entry)
	}
	return entries, nil
}

func (s *mapService) Advertise(ctx context.Context, key string, digest Digest) (bool, error) {
	s.entriesMu.RLock()
	entry, ok := s.entries[key]
	s.entriesMu.RUnlock()
	if !ok {
		if !digest.Tombstone {
			return true, nil
		}
		return false, nil
	} else {
		localTimestamp := meta.NewTimestamp(entry.Digest.Timestamp)
		updateTimestamp := meta.NewTimestamp(digest.Timestamp)
		if updateTimestamp.After(localTimestamp) {
			if digest.Tombstone {
				_, err := s.Update(ctx, Update{
					Entry: Entry{
						Key:    key,
						Digest: digest,
					},
				})
				return false, err
			}
			return true, nil
		}
		return false, nil
	}
}

func (s *mapService) Update(ctx context.Context, update Update) (*Update, error) {
	remoteEntry := update.Entry
	s.entriesMu.RLock()
	localEntry, ok := s.entries[remoteEntry.Key]
	s.entriesMu.RUnlock()
	if remoteEntry.Digest.Tombstone {
		if ok {
			localTimestamp := meta.NewTimestamp(localEntry.Digest.Timestamp)
			remoteTimestamp := meta.NewTimestamp(remoteEntry.Digest.Timestamp)
			if remoteTimestamp.After(localTimestamp) {
				s.entries[remoteEntry.Key] = remoteEntry
				return nil, nil
			} else {
				return &Update{
					Entry: localEntry,
				}, nil
			}
		} else {
			return nil, nil
		}
	} else {
		if ok {
			localTimestamp := meta.NewTimestamp(localEntry.Digest.Timestamp)
			remoteTimestamp := meta.NewTimestamp(remoteEntry.Digest.Timestamp)
			if remoteTimestamp.After(localTimestamp) {
				s.entriesMu.Lock()
				defer s.entriesMu.Unlock()
				localEntry, ok := s.entries[remoteEntry.Key]
				if ok {
					localTimestamp := meta.NewTimestamp(localEntry.Digest.Timestamp)
					remoteTimestamp := meta.NewTimestamp(remoteEntry.Digest.Timestamp)
					if remoteTimestamp.After(localTimestamp) {
						s.entries[remoteEntry.Key] = remoteEntry
						return nil, nil
					} else if localTimestamp.After(remoteTimestamp) {
						return &Update{
							Entry: localEntry,
						}, nil
					} else {
						return nil, nil
					}
				} else {
					s.entries[remoteEntry.Key] = remoteEntry
					return nil, nil
				}
			} else if localTimestamp.After(remoteTimestamp) {
				return &Update{
					Entry: localEntry,
				}, nil
			} else {
				return nil, nil
			}
		} else {
			s.entriesMu.Lock()
			defer s.entriesMu.Unlock()
			localEntry, ok := s.entries[remoteEntry.Key]
			if ok {
				localTimestamp := meta.NewTimestamp(localEntry.Digest.Timestamp)
				remoteTimestamp := meta.NewTimestamp(remoteEntry.Digest.Timestamp)
				if remoteTimestamp.After(localTimestamp) {
					s.entries[remoteEntry.Key] = remoteEntry
					return nil, nil
				} else if localTimestamp.After(remoteTimestamp) {
					return &Update{
						Entry: localEntry,
					}, nil
				} else {
					return nil, nil
				}
			} else {
				s.entries[remoteEntry.Key] = remoteEntry
				return nil, nil
			}
		}
	}
}

func (s *mapService) close() {
	s.cancel()
}

func newPeer(service *mapService, replica *cluster.Replica) (*mapPeer, error) {
	peer := &mapPeer{
		service: service,
		replica: replica,
	}
	err := peer.connect()
	if err != nil {
		return nil, err
	}
	return peer, nil
}

type mapPeer struct {
	service  *mapService
	replica  *cluster.Replica
	conn     *grpc.ClientConn
	stream   MapProtocol_GossipClient
	advertCh chan Advertisement
	updateCh chan Update
}

func (r *mapPeer) connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := r.replica.Connect(ctx, cluster.WithDialOption(grpc.WithInsecure()))
	if err != nil {
		return err
	}
	r.conn = conn
	client := NewMapProtocolClient(conn)
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

func (r *mapPeer) bootstrap() error {
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

func (r *mapPeer) enqueueAdvertisement(advertisement Advertisement) {
	r.advertCh <- advertisement
}

func (r *mapPeer) enqueueUpdate(update Update) {
	r.updateCh <- update
}

func (r *mapPeer) gossip() {
	for {
		select {
		case update := <-r.updateCh:
			r.sendUpdate(update)
		case advert := <-r.advertCh:
			r.sendAdvertisement(advert)
		}
	}
}

func (r *mapPeer) sendUpdate(update Update) {
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

func (r *mapPeer) sendAdvertisement(advert Advertisement) {
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

func (r *mapPeer) close() {
	close(r.advertCh)
	close(r.updateCh)
}
