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

package gossip

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/time"
	"sync"
)

// NewPartition creates a new proxy partition
func NewPartition(p *cluster.Partition, clock time.Clock, registry Registry) *Partition {
	return &Partition{
		Partition: p,
		clock:     clock,
		registry:  registry,
		services:  make(map[ServiceID]Service),
	}
}

// PartitionID is a partition identifier
type PartitionID int

// Partition is a proxy partition
type Partition struct {
	*cluster.Partition
	clock    time.Clock
	registry Registry
	ID       PartitionID
	services map[ServiceID]Service
	mu       sync.RWMutex
}

func (p *Partition) GetService(ctx context.Context, serviceType ServiceType, serviceID ServiceID) (Service, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	service, ok := p.services[serviceID]
	if !ok {
		f, err := p.registry.GetServiceFunc(serviceType)
		if err != nil {
			return nil, err
		}
		service, err = f(ctx, serviceID, p, p.clock)
		if err != nil {
			return nil, err
		}
		p.services[serviceID] = service
	}
	return service, nil
}

func (p *Partition) getReplica(ctx context.Context, serviceType ServiceType, serviceID ServiceID) (Replica, error) {
	service, err := p.GetService(ctx, serviceType, serviceID)
	if err != nil {
		return nil, err
	}
	return service.Replica(), nil
}

func (p *Partition) deleteReplica(serviceType ServiceType, serviceID ServiceID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.services[serviceID]
	if !ok {
		return errors.NewNotFound("service '%s' not found", serviceID)
	}
	delete(p.services, serviceID)
	return nil
}
