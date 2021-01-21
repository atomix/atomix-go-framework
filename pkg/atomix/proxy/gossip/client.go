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
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc/metadata"
)

const (
	primitiveTypeKey = "Primitive-Type"
	primitiveNameKey = "Primitive-Name"
)

// NewClient creates a new proxy client
func NewClient(cluster *cluster.Cluster) *Client {
	partitions := cluster.Partitions()
	proxyPartitions := make([]*Partition, 0, len(partitions))
	proxyPartitionsByID := make(map[PartitionID]*Partition)
	for _, partition := range partitions {
		proxyPartition := NewPartition(cluster, partition)
		proxyPartitions = append(proxyPartitions, proxyPartition)
		proxyPartitionsByID[proxyPartition.ID] = proxyPartition
	}
	return &Client{
		partitions:     proxyPartitions,
		partitionsByID: proxyPartitionsByID,
	}
}

// Client is a client for communicating with the storage layer
type Client struct {
	cluster        *cluster.Cluster
	partitions     []*Partition
	partitionsByID map[PartitionID]*Partition
}

func (p *Client) getPrimitiveName(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.NewInvalid("no metadata found")
	}
	names := md.Get(primitiveNameKey)
	if len(names) != 1 {
		return "", errors.NewInvalid("no primitive name found")
	}
	return names[0], nil
}

func (p *Client) PartitionFrom(ctx context.Context) (*Partition, error) {
	name, err := p.getPrimitiveName(ctx)
	if err != nil {
		return nil, err
	}
	return p.PartitionBy([]byte(name)), nil
}

func (p *Client) Partition(ctx context.Context, partitionID PartitionID) *Partition {
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
