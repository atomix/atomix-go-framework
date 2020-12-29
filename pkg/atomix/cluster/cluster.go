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
	"errors"
	"github.com/atomix/api/go/atomix/storage"
	"sync"
	"time"
)

// NewGroup creates a new peer group
func NewCluster(config storage.StorageConfig, opts ...Option) *Cluster {
	options := applyOptions(opts...)

	var member *Member
	if options.memberID != "" {
		services := options.services
		if services == nil {
			services = []Service{}
		}
		member = NewMember(ReplicaID(options.memberID), options.peerHost, options.peerPort, services...)
	}

	return &Cluster{
		member:   member,
		replicas: make(ReplicaSet),
		options:  *options,
		leaveCh:  make(chan struct{}),
		watchers: make([]chan<- PartitionSet, 0),
	}
}

// Cluster manages the peer group for a client
type Cluster struct {
	member     *Member
	options    options
	replicas   ReplicaSet
	partitions PartitionSet
	watchers   []chan<- PartitionSet
	closer     context.CancelFunc
	leaveCh    chan struct{}
	mu         sync.RWMutex
}

// Member returns the local group member
func (c *Cluster) Member() *Replica {
	return c.member.Replica
}

// Replica returns a replica by ID
func (c *Cluster) Replica(id ReplicaID) *Replica {
	return c.replicas[id]
}

// Replicas returns the current replicas
func (c *Cluster) Replicas() ReplicaSet {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.replicas != nil {
		return c.replicas
	}
	return ReplicaSet{}
}

// Partitions returns the current partitions
func (c *Cluster) Partitions() PartitionSet {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.partitions != nil {
		return c.partitions
	}
	return PartitionSet{}
}

// Update updates the cluster configuration
func (c *Cluster) Update(config storage.StorageConfig) error {
	c.mu.Lock()

	replicaConfigs := make(map[ReplicaID]storage.StorageReplica)
	for _, replicaConfig := range config.Replicas {
		replicaConfigs[ReplicaID(replicaConfig.ID)] = replicaConfig
	}

	for id := range c.replicas {
		if _, ok := replicaConfigs[id]; !ok {
			delete(c.replicas, id)
		}
	}

	for id, replicaConfig := range replicaConfigs {
		if _, ok := c.replicas[id]; !ok {
			c.replicas[id] = NewReplica(replicaConfig)
		}
	}

	partitionConfigs := make(map[PartitionID]storage.StoragePartition)
	for _, partition := range config.Partitions {
		partitionConfigs[PartitionID(partition.PartitionID)] = partition
	}

	for id := range c.partitions {
		if _, ok := partitionConfigs[id]; !ok {
			delete(c.partitions, id)
		}
	}

	for id, partitionConfig := range partitionConfigs {
		partition, ok := c.partitions[id]
		if ok {
			if err := partition.Update(partitionConfig); err != nil {
				c.mu.Unlock()
				return err
			}
		} else {
			c.partitions[id] = NewPartition(partitionConfig, c)
		}
	}

	partitions := c.partitions
	c.mu.Unlock()

	c.mu.RLock()
	for _, watcher := range c.watchers {
		watcher <- partitions
	}
	c.mu.RUnlock()
	return nil
}

// Watch watches the partitions for changes
func (c *Cluster) Watch(ctx context.Context, ch chan<- PartitionSet) error {
	c.mu.Lock()
	watcher := make(chan PartitionSet)
	partitions := c.partitions
	go func() {
		if partitions != nil {
			ch <- partitions
		}
		for {
			select {
			case partitions, ok := <-watcher:
				if !ok {
					return
				}
				ch <- partitions
			case <-ctx.Done():
				c.mu.Lock()
				watchers := make([]chan<- PartitionSet, 0)
				for _, ch := range c.watchers {
					if ch != watcher {
						watchers = append(watchers, ch)
					}
				}
				c.watchers = watchers
				c.mu.Unlock()
				close(watcher)
			}
		}
	}()
	c.watchers = append(c.watchers, watcher)
	c.mu.Unlock()
	return nil
}

// Close closes the group
func (c *Cluster) Close() error {
	c.mu.RLock()
	closer := c.closer
	leaveCh := c.leaveCh
	c.mu.RUnlock()
	timeout := time.Minute
	if closer != nil {
		closer()
		select {
		case <-leaveCh:
			return nil
		case <-time.After(timeout):
			return errors.New("leave timed out")
		}
	}
	return nil
}

// NodeID is a host node identifier
type NodeID string

// ReplicaSet is a set of replicas
type ReplicaSet map[ReplicaID]*Replica

// PartitionSet is a set of partitions
type PartitionSet map[PartitionID]*Partition
