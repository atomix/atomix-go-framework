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
	"github.com/atomix/atomix-go-sdk/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/util/async"
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
