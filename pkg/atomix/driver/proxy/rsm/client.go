// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

import (
	"context"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/env"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util/async"
)

// NewClient creates a new proxy client
func NewClient(cluster cluster.Cluster, env env.DriverEnv) *Client {
	partitions := cluster.Partitions()
	proxyPartitions := make([]*Partition, 0, len(partitions))
	proxyPartitionsByID := make(map[PartitionID]*Partition)
	for _, partition := range partitions {
		proxyPartition := NewPartition(partition)
		proxyPartitions = append(proxyPartitions, proxyPartition)
		proxyPartitionsByID[proxyPartition.ID] = proxyPartition
	}
	return &Client{
		Namespace:      env.Namespace,
		partitions:     proxyPartitions,
		partitionsByID: proxyPartitionsByID,
	}
}

// Client is a client for communicating with the storage layer
type Client struct {
	Namespace      string
	partitions     []*Partition
	partitionsByID map[PartitionID]*Partition
}

func (p *Client) Partition(partitionID PartitionID) *Partition {
	return p.partitionsByID[partitionID]
}

func (p *Client) PartitionBy(partitionKey []byte) *Partition {
	i, err := util.GetPartitionIndex(partitionKey, len(p.partitions))
	if err != nil {
		panic(err)
	}
	return p.partitions[i]
}

func (p *Client) Partitions() []*Partition {
	return p.partitions
}

func (p *Client) Connect(ctx context.Context) error {
	return async.IterAsync(len(p.partitions), func(i int) error {
		return p.partitions[i].connect(ctx)
	})
}

func (p *Client) Close(ctx context.Context) error {
	return async.IterAsync(len(p.partitions), func(i int) error {
		return p.partitions[i].close(ctx)
	})
}
