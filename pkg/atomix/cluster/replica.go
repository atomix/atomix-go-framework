// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"google.golang.org/grpc"
	"sync"
)

// ReplicaID is a peer identifier
type ReplicaID string

// NewReplica returns a new replica
func NewReplica(network Network, config protocolapi.ProtocolReplica) *Replica {
	extraPorts := make(map[string]int)
	for name, port := range config.ExtraPorts {
		extraPorts[name] = int(port)
	}
	return &Replica{
		network:    network,
		ID:         ReplicaID(config.ID),
		NodeID:     NodeID(config.NodeID),
		Host:       config.Host,
		Port:       int(config.APIPort),
		extraPorts: extraPorts,
	}
}

// Replica is a replicas group peer
type Replica struct {
	network    Network
	ID         ReplicaID
	NodeID     NodeID
	Host       string
	Port       int
	extraPorts map[string]int
	conn       *grpc.ClientConn
	mu         sync.RWMutex
}

// GetPort gets a named port
func (m *Replica) GetPort(name string) int {
	return m.extraPorts[name]
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

	var target string
	if options.scheme != "" {
		target = fmt.Sprintf("%s:///%s:%d", options.scheme, m.Host, m.Port)
	} else {
		target = fmt.Sprintf("%s:%d", m.Host, m.Port)
	}

	dialOpts := options.dialOptions
	dialOpts = append(dialOpts, grpc.WithContextDialer(m.network.Connect))
	conn, err := grpc.DialContext(ctx, target, dialOpts...)
	if err != nil {
		return nil, err
	}
	m.conn = conn
	return conn, err
}
