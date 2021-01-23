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
	"github.com/atomix/go-framework/pkg/atomix/time"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc/metadata"
)

// NewClient creates a new proxy client
func NewClient(cluster *cluster.Cluster, scheme time.Scheme) *Client {
	partitions := cluster.Partitions()
	proxyPartitions := make([]*Partition, 0, len(partitions))
	proxyPartitionsByID := make(map[PartitionID]*Partition)
	for _, partition := range partitions {
		proxyPartition := NewPartition(cluster, partition)
		proxyPartitions = append(proxyPartitions, proxyPartition)
		proxyPartitionsByID[proxyPartition.ID] = proxyPartition
	}
	return &Client{
		cluster:        cluster,
		partitions:     proxyPartitions,
		partitionsByID: proxyPartitionsByID,
		clock:          scheme.NewClock(),
	}
}

// Client is a client for communicating with the storage layer
type Client struct {
	cluster        *cluster.Cluster
	partitions     []*Partition
	partitionsByID map[PartitionID]*Partition
	clock          time.Clock
}

func (p *Client) getPrimitiveName(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.NewInvalid("no headers found")
	}
	name, ok := headers.PrimitiveName.GetString(md)
	if !ok {
		return "", errors.NewInvalid("no primitive name header set")
	}
	return name, nil
}

func (p *Client) AddOutgoingMD(md metadata.MD) {
	primitiveType, ok := headers.PrimitiveType.GetString(md)
	if ok {
		headers.ServiceType.SetString(md, primitiveType)
	}
	primitiveName, ok := headers.PrimitiveName.GetString(md)
	if ok {
		headers.ServiceID.SetString(md, primitiveName)
	}
	ts := p.clock.Increment()
	p.clock.Scheme().Codec().EncodeMD(md, ts)
}

func (p *Client) HandleIncomingMD(md metadata.MD) error {
	ts, err := p.clock.Scheme().Codec().DecodeMD(md)
	if err != nil {
		return err
	}
	p.clock.Update(ts)
	return nil
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
