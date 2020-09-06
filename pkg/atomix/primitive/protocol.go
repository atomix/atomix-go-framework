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

package primitive

import (
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"time"
)

// ProtocolContext provides information about the context within which a service is running
type ProtocolContext interface {
	// Node is the local node identifier
	Node() string

	// Index returns the current index of the service
	Index() uint64

	// Timestamp returns a deterministic, monotonically increasing timestamp
	Timestamp() time.Time
}

// Protocol is the interface to be implemented by replication protocols
type Protocol interface {
	ProtocolClient

	// Start starts the protocol
	Start(cluster cluster.Cluster) error

	// Stop stops the protocol
	Stop() error
}
