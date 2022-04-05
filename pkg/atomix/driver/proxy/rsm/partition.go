// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

import (
	"context"
	"fmt"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
)

// NewPartition creates a new proxy partition
func NewPartition(partition cluster.Partition) *Partition {
	return &Partition{
		Session:   NewSession(partition),
		Partition: partition,
		ID:        PartitionID(partition.ID()),
	}
}

// PartitionID is a partition identifier
type PartitionID int

// Partition is a proxy partition
type Partition struct {
	cluster.Partition
	*Session
	ID PartitionID
}

func (p *Partition) connect(ctx context.Context) error {
	log.Infof("Opening session on partition %d", p.ID)
	err := p.Session.open(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("Opening session on partition %d", p.ID))
		return err
	}
	return nil
}

func (p *Partition) close(ctx context.Context) error {
	log.Infof("Disconnecting from partition %d", p.ID)
	err := p.Session.close(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("Disconnecting from partition %d", p.ID))
		return err
	}
	return nil
}
