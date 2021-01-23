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
	"github.com/atomix/go-framework/pkg/atomix/util"
	"google.golang.org/grpc/metadata"
	"strconv"
)

const partitionsKey = "Partitions"
const partitionKey = "Partition"
const serviceTypeKey = "Service-Type"
const serviceIdKey = "Service-Id"

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
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.NewUnavailable("no partitions header found")
	}

	partitionIDs := md.Get(partitionKey)
	if len(partitionIDs) != 1 {
		return nil, errors.NewUnavailable("no partition header found")
	}
	serviceTypes := md.Get(serviceTypeKey)
	if len(serviceTypes) != 1 {
		return nil, errors.NewUnavailable("no service-type header found")
	}
	serviceIDs := md.Get(serviceIdKey)
	if len(serviceIDs) != 1 {
		return nil, errors.NewUnavailable("no service-id header found")
	}

	partitionID, err := strconv.Atoi(partitionIDs[0])
	if err != nil {
		return nil, errors.NewUnavailable("partition header is invalid: %v", err)
	}
	partition, err := m.Partition(PartitionID(partitionID))
	if err != nil {
		return nil, err
	}

	serviceType := ServiceType(serviceTypes[0])
	serviceID := ServiceID(serviceIDs[0])
	return partition.GetService(serviceType, serviceID)
}

func (m *Manager) PartitionFrom(ctx context.Context) (*Partition, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.NewUnavailable("no partitions header found")
	}

	partitionNames := md.Get(partitionKey)
	if len(partitionNames) != 1 {
		return nil, errors.NewUnavailable("no partitions header found")
	}

	partitionID, err := strconv.Atoi(partitionNames[0])
	if err != nil {
		return nil, errors.NewUnavailable("partition header is invalid: %v", err)
	}
	return m.Partition(PartitionID(partitionID))
}

func (m *Manager) PartitionsFrom(ctx context.Context) ([]*Partition, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.NewUnavailable("no partitions header found")
	}

	partitionNames := md.Get(partitionsKey)
	partitions := make([]*Partition, 0, len(partitionNames))
	for _, partitionName := range partitionNames {
		partitionID, err := strconv.Atoi(partitionName)
		if err != nil {
			return nil, errors.NewUnavailable("partition header is invalid: %v", err)
		}
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
