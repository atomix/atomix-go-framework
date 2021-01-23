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
	"google.golang.org/grpc/metadata"
	"sync"
)

// NewPartition creates a new proxy partition
func NewPartition(p *cluster.Partition, registry Registry) *Partition {
	return &Partition{
		Partition: p,
		registry:  registry,
		services: make(map[ServiceID]Service),
	}
}

// PartitionID is a partition identifier
type PartitionID int

// Partition is a proxy partition
type Partition struct {
	*cluster.Partition
	registry Registry
	ID       PartitionID
	services map[ServiceID]Service
	mu       sync.RWMutex
}

func (p *Partition) ServiceFrom(ctx context.Context) (Service, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.NewUnavailable("no partitions header found")
	}

	partitionIDs := md.Get(partitionKey)
	if len(partitionIDs) != 1 {
		return nil, errors.NewUnavailable("no partition header found")
	}
	serviceTypes := md.Get(serviceTypeKey)
	if len(serviceTypes) != 1 {
		return nil, errors.NewUnavailable("no service-type header found")
	}
	serviceIDs := md.Get(serviceIdKey)
	if len(serviceIDs) != 1 {
		return nil, errors.NewUnavailable("no service-id header found")
	}
	serviceType := ServiceType(serviceTypes[0])
	serviceID := ServiceID(serviceIDs[0])
	return p.GetService(serviceType, serviceID)
}

func (p *Partition) GetService(serviceType ServiceType, serviceID ServiceID) (Service, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	service, ok := p.services[serviceID]
	if !ok {
		f, err := p.registry.GetServiceFunc(serviceType)
		if err != nil {
			return nil, err
		}
		service, err = f(serviceID, p)
		if err != nil {
			return nil, err
		}
		p.services[serviceID] = service
	}
	return service, nil
}

func (p *Partition) DeleteService(t ServiceType, serviceID ServiceID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.services[serviceID]
	if !ok {
		return errors.NewNotFound("service '%s' not found", serviceID)
	}
	delete(p.services, serviceID)
	return nil
}
