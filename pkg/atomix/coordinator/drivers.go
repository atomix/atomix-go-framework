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

package coordinator

import (
	coordinatorapi "github.com/atomix/api/go/atomix/management/coordinator"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
)

// newDriverRegistry creates a new driver registry
func newDriverRegistry() *DriverRegistry {
	return &DriverRegistry{
		drivers: make(map[coordinatorapi.DriverId]coordinatorapi.DriverConfig),
	}
}

// DriverRegistry is a driver registry
type DriverRegistry struct {
	drivers map[coordinatorapi.DriverId]coordinatorapi.DriverConfig
	mu      sync.RWMutex
}

func (r *DriverRegistry) AddDriver(driver coordinatorapi.DriverConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.drivers[driver.ID]; ok {
		return errors.NewAlreadyExists("driver '%s' already exists", driver.ID)
	}
	r.drivers[driver.ID] = driver
	return nil
}

func (r *DriverRegistry) RemoveDriver(id coordinatorapi.DriverId) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.drivers[id]; ok {
		return errors.NewNotFound("driver '%s' not found", id)
	}
	delete(r.drivers, id)
	return nil
}

func (r *DriverRegistry) GetDriver(id coordinatorapi.DriverId) (coordinatorapi.DriverConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	driver, ok := r.drivers[id]
	if !ok {
		return coordinatorapi.DriverConfig{}, errors.NewNotFound("driver '%s' not found", id)
	}
	return driver, nil
}

func (r *DriverRegistry) ListDrivers() ([]coordinatorapi.DriverConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	drivers := make([]coordinatorapi.DriverConfig, 0, len(r.drivers))
	for _, driver := range r.drivers {
		drivers = append(drivers, driver)
	}
	return drivers, nil
}
