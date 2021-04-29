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
	"github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/time"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util/async"
)

// NewClient creates a new proxy client
func NewClient(cluster cluster.Cluster, scheme time.Scheme) *Client {
	partitions := cluster.Partitions()
	proxyPartitions := make([]*Partition, 0, len(partitions))
	proxyPartitionsByID := make(map[PartitionID]*Partition)
	clock := scheme.NewClock()
	for _, partition := range partitions {
		proxyPartition := NewPartition(cluster, partition, clock)
		proxyPartitions = append(proxyPartitions, proxyPartition)
		proxyPartitionsByID[proxyPartition.ID] = proxyPartition
	}
	return &Client{
		Cluster:        cluster,
		clock:          clock,
		partitions:     proxyPartitions,
		partitionsByID: proxyPartitionsByID,
	}
}

// Client is a client for communicating with the storage layer
type Client struct {
	Cluster        cluster.Cluster
	clock          time.Clock
	partitions     []*Partition
	partitionsByID map[PartitionID]*Partition
}

func (c *Client) addTimestamp(timestamp *meta.Timestamp) *meta.Timestamp {
	var t time.Timestamp
	if timestamp != nil {
		t = c.clock.Update(time.NewTimestamp(*timestamp))
	} else {
		t = c.clock.Increment()
	}
	proto := c.clock.Scheme().Codec().EncodeTimestamp(t)
	return &proto
}

func (c *Client) AddResponseHeaders(headers *primitive.ResponseHeaders) {
	headers.Timestamp = c.addTimestamp(headers.Timestamp)
}

func (c *Client) Partition(partitionID PartitionID) *Partition {
	return c.partitionsByID[partitionID]
}

func (c *Client) PartitionBy(partitionKey []byte) *Partition {
	i, err := util.GetPartitionIndex(partitionKey, len(c.partitions))
	if err != nil {
		panic(err)
	}
	return c.partitions[i]
}

func (c *Client) Partitions() []*Partition {
	return c.partitions
}

func (c *Client) Close() error {
	return async.IterAsync(len(c.partitions), func(i int) error {
		return c.partitions[i].Close()
	})
}
