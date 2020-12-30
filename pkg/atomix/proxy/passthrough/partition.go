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

package passthrough

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"google.golang.org/grpc"
	"math/rand"
	"sync"
)

// NewPartition creates a new proxy partition
func NewPartition(p *cluster.Partition) *Partition {
	return &Partition{
		Partition: p,
	}
}

// PartitionID is a partition identifier
type PartitionID int

// Partition is a proxy partition
type Partition struct {
	*cluster.Partition
	ID     PartitionID
	conn   *grpc.ClientConn
	leader *cluster.Replica
	mu     sync.RWMutex
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

	if p.leader == nil {
		replicas := make([]*cluster.Replica, 0)
		for _, replica := range p.Replicas() {
			replicas = append(replicas, replica)
		}
		p.leader = replicas[rand.Intn(len(replicas))]
	}

	conn, err := p.leader.Connect(context.Background(), cluster.WithDialOption(grpc.WithInsecure()))
	if err != nil {
		return nil, err
	}
	p.conn = conn
	return conn, nil
}

// Reconnect reconnects the client to the given leader if necessary
func (p *Partition) Reconnect(leader *cluster.Replica) {
	if leader == nil {
		return
	}

	p.mu.RLock()
	connLeader := p.leader
	p.mu.RUnlock()
	if connLeader.ID == leader.ID {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.leader = leader
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
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
