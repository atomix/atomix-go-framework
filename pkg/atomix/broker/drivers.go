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

package broker

import (
	brokerapi "github.com/atomix/api/go/atomix/management/broker"
	"github.com/atomix/go-framework/pkg/atomix/errors"
)

// newDriverRegistry creates a new driver registry
func newDriverRegistry() *DriverRegistry {
	return &DriverRegistry{
		drivers: make(map[brokerapi.DriverId]brokerapi.DriverConfig),
	}
}

// DriverRegistry is a driver registry
// The registry is not thread safe!
type DriverRegistry struct {
	drivers map[brokerapi.DriverId]brokerapi.DriverConfig
}

func (r *DriverRegistry) AddDriver(driver brokerapi.DriverConfig) error {
	if _, ok := r.drivers[driver.ID]; ok {
		return errors.NewAlreadyExists("driver '%s' already exists", driver.ID)
	}
	r.drivers[driver.ID] = driver
	return nil
}

func (r *DriverRegistry) UpdateDriver(driver brokerapi.DriverConfig) error {
	if _, ok := r.drivers[driver.ID]; ok {
		return errors.NewNotFound("driver '%s' not found", driver.ID)
	}
	r.drivers[driver.ID] = driver
	return nil
}

func (r *DriverRegistry) RemoveDriver(id brokerapi.DriverId) error {
	if _, ok := r.drivers[id]; ok {
		return errors.NewNotFound("driver '%s' not found", id)
	}
	delete(r.drivers, id)
	return nil
}

func (r *DriverRegistry) GetDriver(id brokerapi.DriverId) (brokerapi.DriverConfig, error) {
	driver, ok := r.drivers[id]
	if !ok {
		return brokerapi.DriverConfig{}, errors.NewNotFound("driver '%s' not found", id)
	}
	return driver, nil
}

func (r *DriverRegistry) ListDrivers() ([]brokerapi.DriverConfig, error) {
	drivers := make([]brokerapi.DriverConfig, 0, len(r.drivers))
	for _, driver := range r.drivers {
		drivers = append(drivers, driver)
	}
	return drivers, nil
}
