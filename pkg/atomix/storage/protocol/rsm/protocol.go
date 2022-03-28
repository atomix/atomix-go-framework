// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

import (
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
)

// Protocol is the interface to be implemented by replication protocols
type Protocol interface {
	// Partition returns a partition
	Partition(partitionID PartitionID) Partition

	// Partitions returns the protocol partitions
	Partitions() []Partition

	// Start starts the protocol
	Start(cluster cluster.Cluster, registry *Registry) error

	// Stop stops the protocol
	Stop() error
}
