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
	"sync"
)

// NewCluster creates a new cluster
func NewCluster(config protocolapi.ProtocolConfig, opts ...Option) *Cluster {
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

	cluster := &Cluster{
		member:     member,
		replicas:   make(ReplicaSet),
		partitions: make(PartitionSet),
		options:    *options,
		watchers:   make([]chan<- PartitionSet, 0),
	}
	_ = cluster.Update(config)
	return cluster
}

// Cluster manages the peer group for a client
type Cluster struct {
	member     *Member
	options    options
	replicas   ReplicaSet
	partitions PartitionSet
	watchers   []chan<- PartitionSet
	mu         sync.RWMutex
}

// Member returns the local group member
func (c *Cluster) Member() *Member {
	return c.member
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
func (c *Cluster) Update(config protocolapi.ProtocolConfig) error {
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
		if err := partition.Update(partitionConfig); err != nil {
			c.mu.Unlock()
			return err
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

// Close closes the cluster
func (c *Cluster) Close() error {
	if c.member != nil {
		return c.member.Stop()
	}
	return nil
}

// NodeID is a host node identifier
type NodeID string

// ReplicaSet is a set of replicas
type ReplicaSet map[ReplicaID]*Replica

// PartitionSet is a set of partitions
type PartitionSet map[PartitionID]*Partition
