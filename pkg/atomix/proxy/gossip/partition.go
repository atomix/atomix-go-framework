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
	"github.com/atomix/go-framework/pkg/atomix/headers"
	"google.golang.org/grpc"
	"math/rand"
	"sync"
	"time"
)

// NewPartition creates a new proxy partition
func NewPartition(c *cluster.Cluster, p *cluster.Partition) *Partition {
	return &Partition{
		Partition: p,
		cluster:   c,
	}
}

// PartitionID is a partition identifier
type PartitionID int

// Partition is a proxy partition
type Partition struct {
	*cluster.Partition
	cluster *cluster.Cluster
	ID      PartitionID
	conn    *grpc.ClientConn
	mu      sync.RWMutex
}

func (p *Partition) addTimestamp(ctx context.Context) context.Context {
	timestamp, ok := headers.Timestamp.GetTime(ctx)
	if !ok {
		timestamp = time.Now()
	}
	return headers.Timestamp.SetTime(ctx, timestamp)
}

func (p *Partition) addService(ctx context.Context) context.Context {
	primitiveType, ok := headers.PrimitiveType.GetString(ctx)
	if !ok {
		return ctx
	}
	ctx = headers.ServiceType.SetString(ctx, primitiveType)
	primitiveName, ok := headers.PrimitiveName.GetString(ctx)
	if !ok {
		return ctx
	}
	ctx = headers.ServiceID.SetString(ctx, primitiveName)
	return ctx
}

func (p *Partition) addPartitions(ctx context.Context) context.Context {
	return headers.PartitionID.AddInt(ctx, int(p.ID))
}

func (p *Partition) addPartition(ctx context.Context) context.Context {
	return headers.PartitionID.SetInt(ctx, int(p.ID))
}

// AddPartition adds the header for the partition to the given context
func (p *Partition) AddPartition(ctx context.Context) context.Context {
	ctx = p.addService(ctx)
	ctx = p.addTimestamp(ctx)
	ctx = p.addPartition(ctx)
	return ctx
}

// AddPartitions adds the header for the partitions to the given context
func (p *Partition) AddPartitions(ctx context.Context) context.Context {
	ctx = p.addService(ctx)
	ctx = p.addTimestamp(ctx)
	ctx = p.addPartitions(ctx)
	return ctx
}

// Connect gets the connection to the partition
func (p *Partition) Connect() (*grpc.ClientConn, error) {
	p.mu.RLock()
	conn := p.conn
	p.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	conn = p.conn
	if conn != nil {
		return conn, nil
	}

	var replica *cluster.Replica
	replicas := make([]*cluster.Replica, 0, len(p.Replicas()))
	for _, r := range p.Replicas() {
		member, ok := p.cluster.Member()
		if ok && r.NodeID == member.NodeID {
			replica = r
			break
		}
		replicas = append(replicas, r)
	}

	if replica == nil {
		replica = replicas[rand.Intn(len(replicas))]
	}

	conn, err := replica.Connect(context.Background(), cluster.WithDialOption(grpc.WithInsecure()))
	if err != nil {
		return nil, err
	}
	p.conn = conn
	return conn, nil
}

// close closes the connections
func (p *Partition) Close() error {
	p.mu.Lock()
	conn := p.conn
	p.conn = nil
	p.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}