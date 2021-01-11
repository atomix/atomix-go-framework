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

package cluster

import (
	"context"
	"fmt"
	protocolapi "github.com/atomix/api/go/atomix/protocol"
	"google.golang.org/grpc"
	"sync"
)

// ReplicaID is a peer identifier
type ReplicaID string

// NewReplica returns a new replica
func NewReplica(config protocolapi.ProtocolReplica) *Replica {
	return &Replica{
		ID:     ReplicaID(config.ID),
		NodeID: NodeID(config.NodeID),
		Host:   config.Host,
		Port:   int(config.APIPort),
	}
}

// Replica is a replicas group peer
type Replica struct {
	ID     ReplicaID
	NodeID NodeID
	Host   string
	Port   int
	conn   *grpc.ClientConn
	mu     sync.RWMutex
}

// Connect connects to the replica
func (m *Replica) Connect(ctx context.Context, opts ...ConnectOption) (*grpc.ClientConn, error) {
	options := applyConnectOptions(opts...)

	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn != nil {
		return m.conn, nil
	}

	conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", m.Host, m.Port), options.dialOptions...)
	if err != nil {
		return nil, err
	}
	m.conn = conn
	return conn, err
}
