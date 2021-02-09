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
	"github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/api/go/atomix/primitive/meta"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/time"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
)

// NewClient creates a new proxy client
func NewClient(cluster cluster.Cluster, scheme time.Scheme) *Client {
	partitions := cluster.Partitions()
	proxyPartitions := make([]*Partition, 0, len(partitions))
	proxyPartitionsByID := make(map[PartitionID]*Partition)
	for _, partition := range partitions {
		proxyPartition := NewPartition(cluster, partition)
		proxyPartitions = append(proxyPartitions, proxyPartition)
		proxyPartitionsByID[proxyPartition.ID] = proxyPartition
	}
	return &Client{
		Cluster:        cluster,
		partitions:     proxyPartitions,
		partitionsByID: proxyPartitionsByID,
		clock:          scheme.NewClock(),
	}
}

// Client is a client for communicating with the storage layer
type Client struct {
	Cluster        cluster.Cluster
	partitions     []*Partition
	partitionsByID map[PartitionID]*Partition
	clock          time.Clock
}

func (p *Client) prepareTimestamp(timestamp *meta.Timestamp) *meta.Timestamp {
	var t time.Timestamp
	if timestamp != nil {
		t = p.clock.Update(time.NewTimestamp(*timestamp))
	} else {
		t = p.clock.Increment()
	}
	proto := p.clock.Scheme().Codec().EncodeTimestamp(t)
	return &proto
}

func (p *Client) PrepareRequest(headers *primitive.RequestHeaders) {
	headers.Timestamp = p.prepareTimestamp(headers.Timestamp)
}

func (p *Client) PrepareResponse(headers *primitive.ResponseHeaders) {
	headers.Timestamp = p.prepareTimestamp(headers.Timestamp)
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

func (p *Client) Close() error {
	return async.IterAsync(len(p.partitions), func(i int) error {
		return p.partitions[i].Close()
	})
}
