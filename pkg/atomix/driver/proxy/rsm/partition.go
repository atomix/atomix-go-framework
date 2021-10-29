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
	"fmt"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/cluster"
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
