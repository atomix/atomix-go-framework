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
	"io"
	"sync"
)

// NewCluster creates a new cluster
func NewCluster(config protocolapi.ProtocolConfig, opts ...Option) Cluster {
	options := applyOptions(opts...)

	var member *Member
	if options.memberID != "" {
		var replica *protocolapi.ProtocolReplica
		for _, replicaConfig := range config.Replicas {
			if replicaConfig.ID == options.memberID {
				replica = &replicaConfig
				break
			}
		}

		peerHost := options.peerHost
		if peerHost == "" && replica != nil {
			peerHost = replica.Host
		}
		peerPort := options.peerPort
		if peerPort == 0 && replica != nil {
			peerPort = int(replica.APIPort)
		}

		if replica == nil {
			replica = &protocolapi.ProtocolReplica{
				ID:           options.memberID,
				NodeID:       options.nodeID,
				Host:         peerHost,
				APIPort:      int32(peerPort),
				ProtocolPort: int32(peerPort),
			}
		}
		member = NewMember(*replica)
	}

	c := &cluster{
		member:     member,
		replicas:   make(ReplicaSet),
		partitions: make(PartitionSet),
		options:    *options,
		watchers:   make([]chan<- PartitionSet, 0),
	}
	_ = c.Update(config)
	return c
}

// Cluster manages the peer group for a client
type Cluster interface {
	io.Closer
	// Member returns the local cluster member
	Member() (*Member, bool)
	// Replica looks up a replica by ID
	Replica(id ReplicaID) (*Replica, bool)
	// Replicas returns the set of all replicas in the cluster
	Replicas() ReplicaSet
	// Partition looks up a partition by ID
	Partition(id PartitionID) (Partition, bool)
	// Partitions returns the set of all partitions in the cluster
	Partitions() PartitionSet
}

// ConfigurableCluster is an interface for configurable clusters
type ConfigurableCluster interface {
	Update(config protocolapi.ProtocolConfig) error
}

// cluster manages the peer group for a client
type cluster struct {
	member     *Member
	options    options
	replicas   ReplicaSet
	partitions PartitionSet
	watchers   []chan<- PartitionSet
	mu         sync.RWMutex
}

// Member returns the local group member
func (c *cluster) Member() (*Member, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.member == nil {
		return nil, false
	}
	return c.member, true
}

// Replica returns a replica by ID
func (c *cluster) Replica(id ReplicaID) (*Replica, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	replica, ok := c.replicas[id]
	return replica, ok
}

// Replicas returns the current replicas
func (c *cluster) Replicas() ReplicaSet {
	c.mu.RLock()
	defer c.mu.RUnlock()
	copy := make(ReplicaSet)
	for id, replica := range c.replicas {
		copy[id] = replica
	}
	return copy
}

// Partition returns the given partition
func (c *cluster) Partition(id PartitionID) (Partition, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	partition, ok := c.partitions[id]
	return partition, ok
}

// Partitions returns the current partitions
func (c *cluster) Partitions() PartitionSet {
	c.mu.RLock()
	defer c.mu.RUnlock()
	copy := make(PartitionSet)
	for id, partition := range c.partitions {
		copy[id] = partition
	}
	return copy
}

// Update updates the cluster configuration
func (c *cluster) Update(config protocolapi.ProtocolConfig) error {
	c.mu.Lock()

	replicaConfigs := make(map[ReplicaID]protocolapi.ProtocolReplica)
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

	partitionConfigs := make(map[PartitionID]protocolapi.ProtocolPartition)
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
		if !ok {
			partition = NewPartition(partitionConfig, c)
			c.partitions[id] = partition
		}
		if update, ok := partition.(ConfigurablePartition); ok {
			if err := update.Update(partitionConfig); err != nil {
				c.mu.Unlock()
				return err
			}
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
func (c *cluster) Watch(ctx context.Context, ch chan<- PartitionSet) error {
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

// Close closes the cluster
func (c *cluster) Close() error {
	if c.member != nil {
		return c.member.Stop()
	}
	return nil
}

var _ Cluster = &cluster{}
var _ ConfigurableCluster = &cluster{}

// NodeID is a host node identifier
type NodeID string

// ReplicaSet is a set of replicas
type ReplicaSet map[ReplicaID]*Replica

// PartitionSet is a set of partitions
type PartitionSet map[PartitionID]Partition
