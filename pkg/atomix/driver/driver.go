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

package driver

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/agent"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/env"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/server"
	"google.golang.org/grpc"
	"strings"
	"sync"
)

// NewDriver creates a new driver node
func NewDriver(protocol proxy.ProtocolFunc, opts ...Option) *Driver {
	options := applyOptions(opts...)
	return &Driver{
		Server: server.NewServer(cluster.NewCluster(
			protocolapi.ProtocolConfig{},
			cluster.WithMemberID(options.driverID),
			cluster.WithHost(options.host),
			cluster.WithPort(options.port))),
		Env:      env.GetDriverEnv(),
		protocol: protocol,
		agents:   make(map[driverapi.AgentId]*agent.Agent),
		log:      logging.GetLogger("atomix", "driver", strings.ToLower(options.driverID)),
	}
}

// Driver is a driver node
type Driver struct {
	*server.Server
	Env      env.DriverEnv
	protocol proxy.ProtocolFunc
	agents   map[driverapi.AgentId]*agent.Agent
	mu       sync.RWMutex
	log      logging.Logger
}

// Start starts the node
func (d *Driver) Start() error {
	d.Services().RegisterService(func(s *grpc.Server) {
		server := newServer(d)
		driverapi.RegisterDriverServer(s, server)
	})
	if err := d.Server.Start(); err != nil {
		return err
	}
	return nil
}

// Stop stops the node
func (d *Driver) Stop() error {
	return d.Server.Stop()
}

func (d *Driver) startAgent(id driverapi.AgentId, address driverapi.AgentAddress, config driverapi.AgentConfig) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// If the agent already exists, return an AlreadyExists error
	a, ok := d.agents[id]
	if ok {
		return errors.NewAlreadyExists("agent '%s' already exists", id)
	}

	m, _ := d.Cluster.Member()
	c := cluster.NewCluster(
		config.Protocol,
		cluster.WithMemberID(id.Name),
		cluster.WithNodeID(string(m.NodeID)),
		cluster.WithHost(address.Host),
		cluster.WithPort(int(address.Port)))

	a = agent.NewAgent(d.protocol(c, d.Env))

	// Start the agent before adding it to the cache
	if err := a.Start(); err != nil {
		return err
	}
	d.agents[id] = a
	return nil
}

func (d *Driver) configureAgent(id driverapi.AgentId, config driverapi.AgentConfig) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Get the agent from the cache
	// If the agent is not found return a NotFound error
	a, ok := d.agents[id]
	if !ok {
		return errors.NewNotFound("agent '%s' not found", id)
	}

	// Configure the agent with the updated protocol configuration
	if err := a.Configure(config.Protocol); err != nil {
		return err
	}
	return nil
}

func (d *Driver) stopAgent(id driverapi.AgentId) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Get the agent from the cache
	// If the agent is not found return a NotFound error
	a, ok := d.agents[id]
	if !ok {
		return errors.NewNotFound("agent '%s' not found", id)
	}

	// Stop the agent after removing it from the cache
	delete(d.agents, id)
	if err := a.Stop(); err != nil {
		return err
	}
	return nil
}
