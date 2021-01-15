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
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
)

// NewPartition creates a new proxy partition
func NewPartition(p *cluster.Partition, registry Registry) *Partition {
	return &Partition{
		Partition: p,
		registry:  registry,
	}
}

// PartitionID is a partition identifier
type PartitionID int

// Partition is a proxy partition
type Partition struct {
	*cluster.Partition
	registry Registry
	ID       PartitionID
	services map[ServiceType]map[string]Service
	mu       sync.RWMutex
}

func (p *Partition) CreateService(t ServiceType, n string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	services, ok := p.services[t]
	if !ok {
		services = make(map[string]Service)
		p.services[t] = services
	}
	_, ok = services[n]
	if !ok {
		f, err := p.registry.GetServiceFunc(t)
		if err != nil {
			return err
		}
		services[n] = f(n, p.Partition)
	}
	return nil
}

func (p *Partition) DeleteService(t ServiceType, n string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	services, ok := p.services[t]
	if !ok {
		return errors.NewNotFound("service '%s' not found", n)
	}
	_, ok = services[n]
	if !ok {
		return errors.NewNotFound("service '%s' not found", n)
	}
	delete(services, n)
	return nil
}

func (p *Partition) GetService(t ServiceType, n string) (Service, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	services, ok := p.services[t]
	if !ok {
		return nil, errors.NewNotFound("service '%s' not found", n)
	}
	service, ok := services[n]
	if !ok {
		return nil, errors.NewNotFound("service '%s' not found", n)
	}
	return service, nil
}
