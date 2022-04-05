// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	"context"
	"fmt"
	"github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/time"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"math/rand"
	"sync"
)

// newPartitionClient creates a new proxy partition client
func newPartitionClient(c cluster.Cluster, p cluster.Partition) *PartitionClient {
	return &PartitionClient{
		ID:        PartitionID(p.ID()),
		Partition: p,
		cluster:   c,
	}
}

// PartitionID is a partition identifier
type PartitionID int

// partitionClient is a proxy partition client
type PartitionClient struct {
	cluster.Partition
	cluster cluster.Cluster
	ID      PartitionID
	conn    *grpc.ClientConn
	mu      sync.RWMutex
}

// Connect gets the connection to the partition
func (p *PartitionClient) Connect() (*grpc.ClientConn, error) {
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

// Close closes the connections
func (p *PartitionClient) Close() error {
	p.mu.Lock()
	conn := p.conn
	p.conn = nil
	p.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func newPartition(client *PartitionClient, clock time.Clock, replicas int) *Partition {
	return &Partition{
		PartitionClient: client,
		clock:           clock,
		replicas:        replicas,
	}
}

type Partition struct {
	*PartitionClient
	clock    time.Clock
	replicas int
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

func (p *Partition) AddRequestHeaders(ctx context.Context, headers *primitive.RequestHeaders) context.Context {
	headers.Timestamp = p.addTimestamp(headers.Timestamp)
	return metadata.AppendToOutgoingContext(
		ctx,
		"Partition-ID", fmt.Sprint(p.ID),
		"Replication-Factor", fmt.Sprint(p.replicas))
}

func (p *Partition) AddResponseHeaders(headers *primitive.ResponseHeaders) {
	headers.Timestamp = p.addTimestamp(headers.Timestamp)
}
