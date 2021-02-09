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

package drivers

import (
	driverapi "github.com/atomix/api/go/atomix/driver"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
)

// NewRegistry creates a new driver registry
func NewRegistry() *Registry {
	return &Registry{
		drivers: make(map[string]driverapi.DriverMeta),
	}
}

// Registry is a driver registry
type Registry struct {
	drivers map[string]driverapi.DriverMeta
	mu      sync.RWMutex
}

func (r *Registry) RegisterDriver(driver driverapi.DriverMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.drivers[driver.Name]; ok {
		return errors.NewAlreadyExists("driver '%s' already exists", driver.Name)
	}
	r.drivers[driver.Name] = driver
	return nil
}

func (r *Registry) GetDriver(name string) (driverapi.DriverMeta, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	driver, ok := r.drivers[name]
	if !ok {
		return driverapi.DriverMeta{}, errors.NewNotFound("driver '%s' not found", name)
	}
	return driver, nil
}

func (r *Registry) ListDrivers() ([]driverapi.DriverMeta, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	drivers := make([]driverapi.DriverMeta, 0, len(r.drivers))
	for _, driver := range r.drivers {
		drivers = append(drivers, driver)
	}
	return drivers, nil
}
