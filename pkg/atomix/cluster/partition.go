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
	"fmt"
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/errors"
	"google.golang.org/grpc"
	"sync"
)

// PartitionID is a partition identifier
type PartitionID uint32

// NewPartition returns a new replica
func NewPartition(config protocolapi.ProtocolPartition, cluster Cluster) Partition {
	return &partition{
		id:               PartitionID(config.PartitionID),
		cluster:          cluster,
		host:             config.Host,
		port:             int(config.APIPort),
		replicasByID:     make(map[ReplicaID]*Replica),
		readReplicasByID: make(map[ReplicaID]*Replica),
	}
}

// Partition is a cluster partition
type Partition interface {
	// ID returns the partition identifier
	ID() PartitionID
	// Cluster returns the cluster to which the partition belongs
	Cluster() Cluster
	// Member returns the local partition member
	Member() (*Member, bool)
	// Replica looks up a replica in the partition
	Replica(id ReplicaID) (*Replica, bool)
	// Replicas returns the set of all replicas in the partition
	Replicas() []*Replica
	// ReadReplica looks up a read replica in the partition
	ReadReplica(id ReplicaID) (*Replica, bool)
	// ReadReplicas returns the set of all read replicas in the partition
	ReadReplicas() []*Replica
	// Connect connects to the partition
	Connect(ctx context.Context, opts ...ConnectOption) (*grpc.ClientConn, error)
	// Watch watches the partition for changes
	Watch(ctx context.Context, ch chan<- ReplicaSet) error
}

// ConfigurablePartition is an interface for configurable Partitions
type ConfigurablePartition interface {
	Update(protocolapi.ProtocolPartition) error
}

// partition is a cluster partition
type partition struct {
	id               PartitionID
	host             string
	port             int
	cluster          Cluster
	replicas         ReplicaSet
	replicasByID     map[ReplicaID]*Replica
	readReplicas     ReplicaSet
	readReplicasByID map[ReplicaID]*Replica
	watchers         []chan<- ReplicaSet
	configMu         sync.RWMutex
	updateMu         sync.Mutex
	conn             *grpc.ClientConn
	connMu           sync.RWMutex
}

func (p *partition) ID() PartitionID {
	return p.id
}

func (p *partition) Cluster() Cluster {
	return p.cluster
}

// Member returns the local partition member
func (p *partition) Member() (*Member, bool) {
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
func (p *partition) Replica(id ReplicaID) (*Replica, bool) {
	replica, ok := p.replicasByID[id]
	return replica, ok
}

// Replicas returns the current replicas
func (p *partition) Replicas() []*Replica {
	p.configMu.RLock()
	defer p.configMu.RUnlock()
	replicas := make([]*Replica, len(p.replicas))
	copy(replicas, p.replicas)
	return replicas
}

// ReadReplica returns a read replica by ID
func (p *partition) ReadReplica(id ReplicaID) (*Replica, bool) {
	replica, ok := p.readReplicasByID[id]
	return replica, ok
}

// ReadReplicas returns the current read replicas
func (p *partition) ReadReplicas() []*Replica {
	p.configMu.RLock()
	defer p.configMu.RUnlock()
	replicas := make([]*Replica, len(p.readReplicas))
	copy(replicas, p.readReplicas)
	return replicas
}

// Connect connects to the replica
func (p *partition) Connect(ctx context.Context, opts ...ConnectOption) (*grpc.ClientConn, error) {
	options := applyConnectOptions(opts...)

	p.connMu.RLock()
	conn := p.conn
	p.connMu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	p.connMu.Lock()
	defer p.connMu.Unlock()
	if p.conn != nil {
		return p.conn, nil
	}

	var target string
	if options.scheme != "" {
		target = fmt.Sprintf("%s:///%s:%d", options.scheme, p.host, p.port)
	} else {
		target = fmt.Sprintf("%s:%d", p.host, p.port)
	}

	dialOpts := options.dialOptions
	dialOpts = append(dialOpts, grpc.WithContextDialer(p.cluster.Network().Connect))
	conn, err := grpc.DialContext(ctx, target, dialOpts...)
	if err != nil {
		return nil, err
	}
	p.conn = conn
	return conn, err
}

// Update updates the partition configuration
func (p *partition) Update(config protocolapi.ProtocolPartition) error {
	p.updateMu.Lock()
	defer p.updateMu.Unlock()
	p.configMu.Lock()

	replicas := make(ReplicaSet, 0, len(config.Replicas))
	replicasByID := make(map[ReplicaID]*Replica)
	readReplicas := make(ReplicaSet, 0, len(config.ReadReplicas))
	readReplicasByID := make(map[ReplicaID]*Replica)
	for _, id := range config.Replicas {
		replicaID := ReplicaID(id)
		replica, ok := p.cluster.Replica(replicaID)
		if !ok {
			return errors.NewNotFound("replica '%s' not a member of the cluster", replicaID)
		}
		replicas = append(replicas, replica)
		replicasByID[replicaID] = replica
	}

	for _, id := range config.ReadReplicas {
		replicaID := ReplicaID(id)
		replica, ok := p.cluster.Replica(replicaID)
		if !ok {
			return errors.NewNotFound("replica '%s' not a member of the cluster", replicaID)
		}
		readReplicas = append(readReplicas, replica)
		readReplicasByID[replicaID] = replica
	}

	for id := range p.replicasByID {
		if _, ok := replicasByID[id]; !ok {
			delete(p.replicasByID, id)
		}
	}

	for id := range p.readReplicasByID {
		if _, ok := readReplicasByID[id]; !ok {
			delete(p.readReplicasByID, id)
		}
	}

	for id, replica := range replicasByID {
		if _, ok := p.replicasByID[id]; !ok {
			p.replicasByID[id] = replica
		}
	}

	for id, replica := range readReplicasByID {
		if _, ok := p.readReplicasByID[id]; !ok {
			p.readReplicasByID[id] = replica
		}
	}
	p.replicas = replicas
	p.readReplicas = readReplicas
	p.configMu.Unlock()

	p.configMu.RLock()
	for _, watcher := range p.watchers {
		watcher <- replicas
	}
	p.configMu.RUnlock()
	return nil
}

// Watch watches the partition for changes
func (p *partition) Watch(ctx context.Context, ch chan<- ReplicaSet) error {
	p.configMu.Lock()
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
				p.configMu.Lock()
				watchers := make([]chan<- ReplicaSet, 0)
				for _, ch := range p.watchers {
					if ch != watcher {
						watchers = append(watchers, ch)
					}
				}
				p.watchers = watchers
				p.configMu.Unlock()
				close(watcher)
			}
		}
	}()
	p.watchers = append(p.watchers, watcher)
	p.configMu.Unlock()
	return nil
}

var _ Partition = &partition{}
var _ ConfigurablePartition = &partition{}
