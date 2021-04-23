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
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
)

// NewPartition creates a new proxy partition
func NewPartition(partition cluster.Partition, log logging.Logger) *Partition {
	return &Partition{
		Session:   NewSession(partition, log),
		Partition: partition,
		ID:        PartitionID(partition.ID()),
		log:       log,
	}
}

// PartitionID is a partition identifier
type PartitionID int

// Partition is a proxy partition
type Partition struct {
	cluster.Partition
	*Session
	ID  PartitionID
	log logging.Logger
}

func (p *Partition) Connect() error {
	p.log.Infof("Opening session on partition %d", p.ID)
	err := p.Session.open(context.TODO())
	if err != nil {
		p.log.Error(err, fmt.Sprintf("Opening session on partition %d", p.ID))
		return err
	}
	return nil
}

func (p *Partition) Close() error {
	p.log.Infof("Disconnecting from partition %d", p.ID)
	err := p.Session.close(context.TODO())
	_ = p.disconnect()
	if err != nil {
		p.log.Error(err, fmt.Sprintf("Disconnecting from partition %d", p.ID))
		return err
	}
	return nil
}

// PartitionOutput is a result for session-supporting servers containing session header information
type PartitionOutput struct {
	streams.Result
	Type    rsm.SessionResponseType
	Status  rsm.SessionResponseStatus
	Context rsm.SessionResponseContext
}
