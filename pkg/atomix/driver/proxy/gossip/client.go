// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util/async"
)

// NewClient creates a new proxy client
func NewClient(cluster cluster.Cluster) *Client {
	partitions := cluster.Partitions()
	proxyPartitions := make([]*PartitionClient, 0, len(partitions))
	proxyPartitionsByID := make(map[PartitionID]*PartitionClient)
	for _, partition := range partitions {
		proxyPartition := newPartitionClient(cluster, partition)
		proxyPartitions = append(proxyPartitions, proxyPartition)
		proxyPartitionsByID[proxyPartition.ID] = proxyPartition
	}
	return &Client{
		Cluster:    cluster,
		partitions: proxyPartitions,
	}
}

// Client is a client for communicating with the storage layer
type Client struct {
	Cluster    cluster.Cluster
	partitions []*PartitionClient
}

func (c *Client) Close() error {
	return async.IterAsync(len(c.partitions), func(i int) error {
		return c.partitions[i].Close()
	})
}
