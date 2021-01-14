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

package cluster

import (
	"context"
	protocolapi "github.com/atomix/api/go/atomix/protocol"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
)

// PartitionID is a partition identifier
type PartitionID uint32

// NewPartition returns a new replica
func NewPartition(config protocolapi.ProtocolPartition, cluster *Cluster) *Partition {
	return &Partition{
		ID:       PartitionID(config.PartitionID),
		cluster:  cluster,
		replicas: make(map[ReplicaID]*Replica),
	}
}

// Partition is a cluster partition
type Partition struct {
	ID       PartitionID
	cluster  *Cluster
	replicas map[ReplicaID]*Replica
	watchers []chan<- ReplicaSet
	mu       sync.RWMutex
}

// Member returns the local partition member
func (p *Partition) Member() (*Member, bool) {
	member, ok := p.cluster.Member()
	if !ok {
		return nil, false
	}
	_, ok = p.Replica(member.ID)
	if !ok {
		return nil, false
	}
	return member, true
}

// Replica returns a replica by ID
func (p *Partition) Replica(id ReplicaID) (*Replica, bool) {
	replica, ok := p.replicas[id]
	return replica, ok
}

// Replicas returns the current replicas
func (p *Partition) Replicas() ReplicaSet {
	p.mu.RLock()
	defer p.mu.RUnlock()
	copy := make(ReplicaSet)
	for id, replica := range p.replicas {
		copy[id] = replica
	}
	return copy
}

// Update updates the partition configuration
func (p *Partition) Update(config protocolapi.ProtocolPartition) error {
	p.mu.Lock()

	replicas := make(map[ReplicaID]*Replica)
	for _, id := range config.Replicas {
		replicaID := ReplicaID(id)
		replica, ok := p.cluster.Replica(replicaID)
		if !ok {
			return errors.NewNotFound("replica '%s' not a member of the cluster", replicaID)
		}
		replicas[ReplicaID(replicaID)] = replica
	}

	for id := range p.replicas {
		if _, ok := replicas[id]; !ok {
			delete(p.replicas, id)
		}
	}

	for id, replica := range replicas {
		if _, ok := p.replicas[id]; !ok {
			p.replicas[id] = replica
		}
	}
	p.mu.Unlock()

	p.mu.RLock()
	for _, watcher := range p.watchers {
		watcher <- replicas
	}
	p.mu.RUnlock()
	return nil
}

// Watch watches the partition for changes
func (p *Partition) Watch(ctx context.Context, ch chan<- ReplicaSet) error {
	p.mu.Lock()
	watcher := make(chan ReplicaSet)
	replicas := p.replicas
	go func() {
		if replicas != nil {
			ch <- replicas
		}
		for {
			select {
			case replicas, ok := <-watcher:
				if !ok {
					return
				}
				ch <- replicas
			case <-ctx.Done():
				p.mu.Lock()
				watchers := make([]chan<- ReplicaSet, 0)
				for _, ch := range p.watchers {
					if ch != watcher {
						watchers = append(watchers, ch)
					}
				}
				p.watchers = watchers
				p.mu.Unlock()
				close(watcher)
			}
		}
	}()
	p.watchers = append(p.watchers, watcher)
	p.mu.Unlock()
	return nil
}
