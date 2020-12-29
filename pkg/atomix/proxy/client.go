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

package proxy

import (
	storageapi "github.com/atomix/api/go/atomix/storage"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
)

// NewClient creates a new proxy client
func NewClient(config storageapi.StorageConfig) *Client {
	partitions := make([]*Partition, 0, len(config.Partitions))
	partitionsByID := make(map[PartitionID]*Partition)
	for _, partitionConfig := range config.Partitions {
		partition := NewPartition(partitionConfig)
		partitions = append(partitions, partition)
		partitionsByID[partition.ID] = partition
	}
	return &Client{
		partitions:     partitions,
		partitionsByID: partitionsByID,
	}
}

// Client is a client for communicating with the storage layer
type Client struct {
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

func (p *Client) PartitionFor(primitiveID storageapi.PrimitiveId) *Partition {
	return p.PartitionBy([]byte(primitiveID.String()))
}

func (p *Client) Partitions() []*Partition {
	return p.partitions
}

func (p *Client) Connect(cluster cluster.Cluster) error {
	return async.IterAsync(len(p.partitions), func(i int) error {
		return p.partitions[i].Connect(cluster)
	})
}

func (p *Client) Close() error {
	return async.IterAsync(len(p.partitions), func(i int) error {
		return p.partitions[i].Close()
	})
}
