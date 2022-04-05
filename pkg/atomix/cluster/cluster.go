// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"io"
	"sort"
	"sync"
)

// NewCluster creates a new cluster
func NewCluster(network Network, config protocolapi.ProtocolConfig, opts ...Option) Cluster {
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
				ID:         options.memberID,
				NodeID:     options.nodeID,
				Host:       peerHost,
				APIPort:    int32(peerPort),
				ExtraPorts: map[string]int32{},
			}
		}
		member = NewMember(network, *replica)
	}

	c := &cluster{
		network:        network,
		member:         member,
		replicasByID:   make(map[ReplicaID]*Replica),
		partitionsByID: make(map[PartitionID]Partition),
		options:        *options,
		watchers:       make([]chan<- PartitionSet, 0),
	}
	_ = c.Update(config)
	return c
}

// Cluster manages the peer group for a client
type Cluster interface {
	io.Closer
	// Network returns the cluster Network
	Network() Network
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
	network        Network
	member         *Member
	options        options
	replicas       ReplicaSet
	replicasByID   map[ReplicaID]*Replica
	partitions     PartitionSet
	partitionsByID map[PartitionID]Partition
	watchers       []chan<- PartitionSet
	configMu       sync.RWMutex
	updateMu       sync.Mutex
}

// Network returns the cluster Network
func (c *cluster) Network() Network {
	return c.network
}

// Member returns the local group member
func (c *cluster) Member() (*Member, bool) {
	c.configMu.RLock()
	defer c.configMu.RUnlock()
	if c.member == nil {
		return nil, false
	}
	return c.member, true
}

// Replica returns a replica by ID
func (c *cluster) Replica(id ReplicaID) (*Replica, bool) {
	c.configMu.RLock()
	defer c.configMu.RUnlock()
	replica, ok := c.replicasByID[id]
	return replica, ok
}

// Replicas returns the current replicas
func (c *cluster) Replicas() ReplicaSet {
	c.configMu.RLock()
	defer c.configMu.RUnlock()
	replicas := make([]*Replica, len(c.replicas))
	copy(replicas, c.replicas)
	return replicas
}

// Partition returns the given partition
func (c *cluster) Partition(id PartitionID) (Partition, bool) {
	c.configMu.RLock()
	defer c.configMu.RUnlock()
	partition, ok := c.partitionsByID[id]
	return partition, ok
}

// Partitions returns the current partitions
func (c *cluster) Partitions() PartitionSet {
	c.configMu.RLock()
	defer c.configMu.RUnlock()
	partitions := make([]Partition, len(c.partitions))
	copy(partitions, c.partitions)
	return partitions
}

// Update updates the cluster configuration
func (c *cluster) Update(config protocolapi.ProtocolConfig) error {
	c.updateMu.Lock()
	defer c.updateMu.Unlock()

	replicaConfigs := make(map[ReplicaID]protocolapi.ProtocolReplica)
	for _, replicaConfig := range config.Replicas {
		replicaConfigs[ReplicaID(replicaConfig.ID)] = replicaConfig
	}

	c.configMu.Lock()
	for id := range c.replicasByID {
		if _, ok := replicaConfigs[id]; !ok {
			delete(c.replicasByID, id)
		}
	}

	replicas := make(ReplicaSet, 0, len(config.Replicas))
	for id, replicaConfig := range replicaConfigs {
		replica, ok := c.replicasByID[id]
		if !ok {
			replica = NewReplica(c.network, replicaConfig)
			c.replicasByID[id] = replica
		}
		replicas = append(replicas, replica)
	}

	partitionConfigs := make(map[PartitionID]protocolapi.ProtocolPartition)
	for _, partition := range config.Partitions {
		partitionConfigs[PartitionID(partition.PartitionID)] = partition
	}

	partitions := make(PartitionSet, 0, len(config.Partitions))
	for id := range c.partitionsByID {
		if _, ok := partitionConfigs[id]; !ok {
			delete(c.partitionsByID, id)
		}
	}

	for id, partitionConfig := range partitionConfigs {
		partition, ok := c.partitionsByID[id]
		if !ok {
			partition = NewPartition(partitionConfig, c)
			c.partitionsByID[id] = partition
		}
		partitions = append(partitions, partition)
	}

	sort.Slice(replicas, func(i, j int) bool {
		return replicas[i].ID < replicas[j].ID
	})

	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].ID() < partitions[j].ID()
	})

	c.replicas = replicas
	c.partitions = partitions
	c.configMu.Unlock()

	for _, partition := range partitions {
		if config, ok := partitionConfigs[partition.ID()]; ok {
			if update, ok := partition.(ConfigurablePartition); ok {
				if err := update.Update(config); err != nil {
					return err
				}
			}
		}
	}

	c.configMu.RLock()
	for _, watcher := range c.watchers {
		watcher <- c.partitions
	}
	c.configMu.RUnlock()
	return nil
}

// Watch watches the partitions for changes
func (c *cluster) Watch(ctx context.Context, ch chan<- PartitionSet) error {
	c.configMu.Lock()
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
				c.configMu.Lock()
				watchers := make([]chan<- PartitionSet, 0)
				for _, ch := range c.watchers {
					if ch != watcher {
						watchers = append(watchers, ch)
					}
				}
				c.watchers = watchers
				c.configMu.Unlock()
				close(watcher)
			}
		}
	}()
	c.watchers = append(c.watchers, watcher)
	c.configMu.Unlock()
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
type ReplicaSet []*Replica

// PartitionSet is a set of partitions
type PartitionSet []Partition
