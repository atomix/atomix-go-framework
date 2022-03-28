// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	"context"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util"
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
