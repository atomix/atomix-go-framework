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

package rsm

import (
	"context"
	"github.com/atomix/atomix-go-framework/pkg/atomix/stream"
)

// PartitionID is a partition identifier
type PartitionID int

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
