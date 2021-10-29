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
	"github.com/atomix/atomix-go-sdk/pkg/atomix/util"
	"google.golang.org/grpc/metadata"
	"strconv"
)

// newManager creates a new CRDT manager
func newManager(cluster cluster.Cluster, registry *Registry) *Manager {
	partitions := cluster.Partitions()
	proxyPartitions := make([]*Partition, 0, len(partitions))
	proxyPartitionsByID := make(map[PartitionID]*Partition)
	for _, partition := range partitions {
		proxyPartition := NewPartition(partition, registry)
		proxyPartitions = append(proxyPartitions, proxyPartition)
		proxyPartitionsByID[proxyPartition.ID] = proxyPartition
	}
	return &Manager{
		Cluster:        cluster,
		partitions:     proxyPartitions,
		partitionsByID: proxyPartitionsByID,
	}
}

// Manager is a manager for CRDT primitives
type Manager struct {
	Cluster        cluster.Cluster
	partitions     []*Partition
	partitionsByID map[PartitionID]*Partition
}

func (m *Manager) Partition(partitionID PartitionID) (*Partition, error) {
	return m.getPartitionIfMember(m.partitionsByID[partitionID])
}

func (m *Manager) PartitionBy(partitionKey []byte) (*Partition, error) {
	i, err := util.GetPartitionIndex(partitionKey, len(m.partitions))
	if err != nil {
		return nil, errors.NewInternal("could not compute partition index: %v", err)
	}
	return m.partitions[i], nil
}

func (m *Manager) PartitionFrom(ctx context.Context) (*Partition, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.NewInvalid("cannot retrieve headers from context")
	}
	ids := md.Get("Partition-ID")
	if len(ids) != 1 {
		return nil, errors.NewInvalid("cannot retrieve partition ID from headers")
	}
	id := ids[0]
	i, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	return m.Partition(PartitionID(i))
}

func (m *Manager) PartitionFor(serviceID ServiceId) (*Partition, error) {
	return m.PartitionBy([]byte(serviceID.String()))
}

func (m *Manager) getPartitionIfMember(partition *Partition) (*Partition, error) {
	if _, ok := partition.Member(); !ok {
		return nil, errors.NewUnavailable("replica is not a member of partition %d", partition.ID)
	}
	return partition, nil
}
