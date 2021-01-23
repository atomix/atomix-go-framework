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
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/headers"
	"github.com/atomix/go-framework/pkg/atomix/util"
)

// newManager creates a new CRDT manager
func newManager(cluster *cluster.Cluster, registry Registry) *Manager {
	partitions := cluster.Partitions()
	proxyPartitions := make([]*Partition, 0, len(partitions))
	proxyPartitionsByID := make(map[PartitionID]*Partition)
	for _, partition := range partitions {
		proxyPartition := NewPartition(partition, registry)
		proxyPartitions = append(proxyPartitions, proxyPartition)
		proxyPartitionsByID[proxyPartition.ID] = proxyPartition
	}
	return &Manager{
		partitions:     proxyPartitions,
		partitionsByID: proxyPartitionsByID,
	}
}

// Manager is a manager for CRDT primitives
type Manager struct {
	Cluster        *cluster.Cluster
	partitions     []*Partition
	partitionsByID map[PartitionID]*Partition
}

func (m *Manager) ServiceFrom(ctx context.Context) (Service, error) {
	partitionID, ok := headers.PartitionID.GetInt(ctx)
	if !ok {
		return nil, errors.NewUnavailable("no %s header found", headers.PartitionID.Name())
	}
	serviceType, ok := headers.ServiceType.GetString(ctx)
	if !ok {
		return nil, errors.NewUnavailable("no %s header found", headers.ServiceType.Name())
	}
	serviceID, ok := headers.ServiceID.GetString(ctx)
	if !ok {
		return nil, errors.NewUnavailable("no %s header found", headers.ServiceID.Name())
	}

	partition, err := m.Partition(PartitionID(partitionID))
	if err != nil {
		return nil, err
	}

	replica, err := partition.getReplica(ServiceType(serviceType), ServiceID(serviceID))
	if err != nil {
		return nil, err
	}
	return replica.Service(), nil
}

func (m *Manager) PartitionFrom(ctx context.Context) (*Partition, error) {
	partitionID, ok := headers.PartitionID.GetInt(ctx)
	if !ok {
		return nil, errors.NewUnavailable("no %s header found", headers.PartitionID.Name())
	}
	return m.Partition(PartitionID(partitionID))
}

func (m *Manager) PartitionsFrom(ctx context.Context) ([]*Partition, error) {
	partitionIDs, ok := headers.PartitionID.GetInts(ctx)
	if !ok {
		return nil, errors.NewUnavailable("no %s header found", headers.PartitionID.Name())
	}

	partitions := make([]*Partition, 0, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		partition, err := m.Partition(PartitionID(partitionID))
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, partition)
	}
	return partitions, nil
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

func (m *Manager) PartitionFor(serviceID ServiceID) (*Partition, error) {
	return m.PartitionBy([]byte(serviceID))
}

func (m *Manager) getPartitionIfMember(partition *Partition) (*Partition, error) {
	if _, ok := partition.Member(); !ok {
		return nil, errors.NewUnavailable("replica is not a member of partition %d", partition.ID)
	}
	return partition, nil
}