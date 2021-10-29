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

package gossip

import (
	"context"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/errors"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/meta"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/time"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/util/async"
	"sync"
)

func NewPeerGroup(partition *Partition, serviceID ServiceId, clock time.Clock, replicas int) (*PeerGroup, error) {
	var localMemberID MemberID
	member, ok := partition.Partition.Member()
	if ok {
		localMemberID = MemberID(member.ID)
	}
	group := &PeerGroup{
		memberID:  localMemberID,
		partition: partition,
		serviceID: serviceID,
		clock:     clock,
		replicas:  replicas,
	}
	if err := group.start(); err != nil {
		return nil, err
	}
	return group, nil
}

type PeerGroup struct {
	memberID  MemberID
	partition *Partition
	serviceID ServiceId
	peersByID map[PeerID]*Peer
	peers     []*Peer
	peersMu   sync.RWMutex
	clock     time.Clock
	replicas  int
	cancel    context.CancelFunc
}

func (g *PeerGroup) start() error {
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	watchCh := make(chan cluster.ReplicaSet)
	err := g.partition.Watch(ctx, watchCh)
	if err != nil {
		return err
	}

	replicaSet := g.partition.Replicas()
	err = g.updatePeers(replicaSet)
	if err != nil {
		return err
	}

	go g.watchReplicas(watchCh)
	return nil
}

func (g *PeerGroup) watchReplicas(watchCh <-chan cluster.ReplicaSet) {
	for replicaSet := range watchCh {
		err := g.updatePeers(replicaSet)
		if err != nil {
			log.Errorf("Failed to handle replica set change: %v", err)
		}
	}
}

func (g *PeerGroup) updatePeers(replicas cluster.ReplicaSet) error {
	g.peersMu.Lock()
	defer g.peersMu.Unlock()

	replicationFactor := g.replicas
	if replicationFactor == 0 || replicationFactor > len(replicas) {
		replicationFactor = len(replicas)
	}

	peers := make([]*Peer, 0, len(g.peers)+1)
	peersByID := make(map[PeerID]*Peer)
	for i := 0; i < replicationFactor; i++ {
		replica := replicas[i]
		if member, ok := g.partition.Member(); !ok || replica.ID == member.ID {
			continue
		}
		peer, ok := g.peersByID[PeerID(replica.ID)]
		if !ok {
			p, err := newPeer(g, replica, g.clock)
			if err != nil {
				return err
			}
			peer = p
		}
		peers = append(peers, peer)
		peersByID[peer.ID] = peer
	}

	g.peers = peers
	g.peersByID = peersByID
	return nil
}

func (g *PeerGroup) MemberID() MemberID {
	return g.memberID
}

func (g *PeerGroup) Peer(id PeerID) *Peer {
	return g.peersByID[id]
}

func (g *PeerGroup) Peers() []*Peer {
	g.peersMu.RLock()
	defer g.peersMu.RUnlock()
	return g.peers
}

func (g *PeerGroup) Read(ctx context.Context, key string) ([]Object, error) {
	g.peersMu.RLock()
	peers := g.peers
	g.peersMu.RUnlock()

	results, err := async.ExecuteAsync(len(peers), func(i int) (interface{}, error) {
		peer := peers[i]
		return peer.Read(ctx, key)
	})
	if err != nil {
		return nil, errors.From(err)
	}

	objects := make([]Object, 0, len(results))
	for _, result := range results {
		if result != nil {
			objects = append(objects, *result.(*Object))
		}
	}
	return objects, nil
}

func (g *PeerGroup) ReadAll(ctx context.Context, ch chan<- Object) error {
	g.peersMu.RLock()
	peers := g.peers
	g.peersMu.RUnlock()

	wg := &sync.WaitGroup{}
	wg.Add(len(peers))
	err := async.IterAsync(len(peers), func(i int) error {
		peer := peers[i]
		peerCh := make(chan Object)
		err := peer.ReadAll(ctx, peerCh)
		if err != nil {
			return err
		}
		for object := range peerCh {
			ch <- object
		}
		wg.Done()
		return nil
	})
	if err != nil {
		return errors.From(err)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()
	return nil
}

func (g *PeerGroup) Advertise(ctx context.Context, key string, digest meta.ObjectMeta) {
	g.peersMu.RLock()
	peers := g.peers
	g.peersMu.RUnlock()
	for _, peer := range peers {
		peer.Advertise(ctx, key, digest)
	}
}

func (g *PeerGroup) Update(ctx context.Context, object *Object) {
	g.peersMu.RLock()
	peers := g.peers
	g.peersMu.RUnlock()
	for _, peer := range peers {
		peer.Update(ctx, object)
	}
}

func (g *PeerGroup) Close() {
	g.cancel()
}
