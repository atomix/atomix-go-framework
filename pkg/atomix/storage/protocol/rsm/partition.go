// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

import (
	"context"
	"github.com/atomix/atomix-go-framework/pkg/atomix/stream"
)

// PartitionID is a partition identifier
type PartitionID uint32

// PartitionConfig is the partition configuration
type PartitionConfig struct {
	Leader    string
	Followers []string
}

// Partition is the interface for a partition client
type Partition interface {
	// MustLeader returns whether the client can only be used on the leader
	MustLeader() bool

	// IsLeader returns whether the client is the leader
	IsLeader() bool

	// Leader returns the current leader
	Leader() string

	// Followers returns the followers
	Followers() []string

	// WatchConfig watches the partition configuration for changes
	WatchConfig(ctx context.Context, ch chan<- PartitionConfig) error

	// SyncCommand executes a write request
	SyncCommand(ctx context.Context, input []byte, stream stream.WriteStream) error

	// SyncQuery executes a read request
	SyncQuery(ctx context.Context, input []byte, stream stream.WriteStream) error

	// StaleQuery executes a read request
	StaleQuery(ctx context.Context, input []byte, stream stream.WriteStream) error
}
