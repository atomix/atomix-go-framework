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
	"github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/time"
	"google.golang.org/grpc"
	"math/rand"
	"sync"
)

// NewPartition creates a new proxy partition
func NewPartition(c cluster.Cluster, p cluster.Partition, clock time.Clock) *Partition {
	return &Partition{
		ID:        PartitionID(p.ID()),
		Partition: p,
		cluster:   c,
		clock:     clock,
	}
}

// PartitionID is a partition identifier
type PartitionID int

// Partition is a proxy partition
type Partition struct {
	cluster.Partition
	cluster cluster.Cluster
	clock   time.Clock
	ID      PartitionID
	conn    *grpc.ClientConn
	mu      sync.RWMutex
}

func (p *Partition) addTimestamp(timestamp *meta.Timestamp) *meta.Timestamp {
	var t time.Timestamp
	if timestamp != nil {
		t = p.clock.Update(time.NewTimestamp(*timestamp))
	} else {
		t = p.clock.Increment()
	}
	proto := p.clock.Scheme().Codec().EncodeTimestamp(t)
	return &proto
}

func (p *Partition) AddRequestHeaders(headers *primitive.RequestHeaders) {
	headers.PartitionID = uint32(p.ID)
	headers.Timestamp = p.addTimestamp(headers.Timestamp)
}

func (p *Partition) AddResponseHeaders(headers *primitive.ResponseHeaders) {
	headers.Timestamp = p.addTimestamp(headers.Timestamp)
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
